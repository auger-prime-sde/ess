#!/usr/bin/env python

"""
 ESS procedure
 main program
"""

import os
import sys
import re
import json
import logging
import logging.config
import logging.handlers
import threading
import multiprocessing
from datetime import datetime, timedelta
import queue
import shutil
from serial import Serial, SerialException

# ESS stuff
from timer import Timer, periodic_ticker, EvtDisp
from modbus import Binder, Modbus, ModbusError
from logger import LogHandlerRamp, LogHandlerPickle, LogHandlerGrafana
from logger import LogHandlerVoltramp, DataLogger
from logger import makeDLtemperature, makeDLslowcontrol, makeDLcurrents
from logger import makeDLpedenoise, makeDLstat
from logger import makeDLhsampli, makeDLfampli, makeDLlinear
from logger import makeDLfreqgain  # , makeDLcutoff
from logger import QueDispatch, QLogHandler
from BME import BME, TrigDelay, PowerControl, readSerRE, SerialReadTimeout
from UUB import UUBdaq, UUBlisten, UUBtelnet, UUBtsc
from UUB import uubnum2mac, VIRGINUUBNUM
from chamber import Chamber, ESSprogram
from dataproc import DataProcessor, DirectGain, SplitterGain, make_notcalc
from dataproc import make_DPfilter_linear, make_DPfilter_ramp
from dataproc import make_DPfilter_cutoff, make_DPfilter_stat
from afg import AFG, RPiTrigger
from power import PowerSupply
from flir import FLIR
from db import DBconnector

VERSION = '20200204'


class DetectUSB(object):
    """Try to detect USB devices in devlist"""
    re_TTYUSB = re.compile(r'ttyUSB\d+')
    re_TTYACM = re.compile(r'ttyACM\d+')
    re_USBTMC = re.compile(r'usbtmc(?P<tmcid>\d+)')
    SERIALS = {
        "BME": ('ttyUSB', 115200, None, BME.re_bmeinit),
        "trigdelay": ('ttyUSB', 115200, None, TrigDelay.re_init),
        "powercontrol": ('ttyACM', 115200, None, PowerControl.re_init),
        "power_cpx": ('ttyACM', 9600, b'*IDN?\n', PowerSupply.re_cpx),
        "power_hmp": ('ttyACM', 9600, b'*IDN?\n', PowerSupply.re_hmp)}
    TMCS = {
        "afg": (b'*IDN?', re.compile(rb'.*AFG')),
        "mdo": (b'*IDN?', re.compile(rb'.*MDO'))}

    def __init__(self):
        self.found = {}
        self.failed = []
        devfiles = os.listdir('/dev')
        self.devices = {
            'ttyUSB': ['/dev/' + f for f in devfiles
                       if DetectUSB.re_TTYUSB.match(f)],
            'ttyACM': ['/dev/' + f for f in devfiles
                       if DetectUSB.re_TTYACM.match(f)],
            'usbtmc': list(filter(DetectUSB.re_USBTMC.match, devfiles))}
        self.logger = logging.getLogger('DetectUSB')
        for devclass in ('ttyUSB', 'ttyACM', 'usbtmc'):
            self.logger.debug('scanned %s: %s', devclass,
                              ', '.join(self.devices[devclass]))

    def detect(self, devlist, trials=3):
        for trial in range(trials):
            self.logger.info('detect trial %d', trial+1)
            self._detect(devlist)
            self.logger.debug('found: %s', ', '.join(self.found.keys()))
            if not self.failed:
                return self.found
            self.logger.debug('trial %d finished, not found yet: %s',
                              trial+1, ', '.join(self.failed))
            devlist = self.failed
        raise RuntimeError(
            'USB devices not found: %s' % ', '.join(self.failed))

    def _detect(self, devlist):
        self.failed = []
        for dev in ('BME', 'trigdelay', 'powercontrol',
                    'power_cpx', 'power_hmp'):
            if dev in devlist:
                devclass, baudrate, cmd_id, re_resp = DetectUSB.SERIALS[dev]
                for port in self.devices[devclass]:
                    self.logger.debug('Detecting %s on %s @ %d',
                                      dev, port, baudrate)
                    if self._check_serial(port, baudrate, cmd_id, re_resp):
                        self.found[dev] = port
                        self.devices[devclass].remove(port)
                        self.logger.debug('%s found at %s', dev, port)
                        break
                    else:
                        self.logger.debug('%s not at %s', dev, port)
                else:
                    self.failed.append(dev)
                    self.logger.debug('%s not found', dev)

        for dev in DetectUSB.TMCS.keys():
            if dev in devlist:
                cmd_id, re_resp = DetectUSB.TMCS[dev]
                for fn in self.devices['usbtmc']:
                    self.logger.debug('Detecting %s as %s', dev, fn)
                    tmcid = self._check_tmc(fn, cmd_id, re_resp)
                    if tmcid is not None:
                        self.found[dev] = tmcid
                        self.devices['usbtmc'].remove(fn)
                        self.logger.debug('%s found as %s', dev, fn)
                        break
                    else:
                        self.logger.debug('%s not %s', dev, fn)
                else:
                    self.failed.append(dev)
                    self.logger.debug('%s not found', dev)

        if 'chamber' in devlist:  # detect Binder
            modbus = None
            for port in self.devices['ttyUSB']:
                self.logger.debug('Detecting chamber on %s', port)
                try:
                    modbus = Modbus(port)
                    modbus.read_holding_registers(Binder.ADDR_MODE)
                except ModbusError:
                    continue
                finally:
                    if modbus is not None:
                        modbus.__del__()
                self.found['chamber'] = port
                self.devices['ttyUSB'].remove(port)
                self.logger.debug('chamber found at %s', port)
                break

        if 'flir' in devlist:  # detect FLIR
            flir = None
            for port in self.devices['ttyUSB']:
                self.logger.debug('Detecting flir on %s', port)
                try:
                    flir = FLIR(port, None, None, None)
                    self.found['flir'] = port
                    self.devices['ttyUSB'].remove(port)
                    self.logger.debug('flir found at %s', port)
                    flir.__del__()
                    break
                except SerialReadTimeout:
                    if flir is not None:
                        flir.__del__()
                        flir = None
                    self.logger.debug('flir not at %s', port)
            else:
                self.failed.append('flir')
                self.logger.debug('flir not found')

    def _check_serial(self, port, baudrate, cmd_id, re_resp):
        ser = None
        try:
            ser = Serial(port, baudrate,
                         bytesize=8, parity='N', stopbits=1, timeout=0.5)
            if cmd_id is not None:
                ser.write(cmd_id)
            readSerRE(ser, re_resp, timeout=2.0, logger=self.logger)
            return True
        except (SerialReadTimeout, SerialException, OSError):
            return False
        except Exception:
            self.logger.exception('_check_serial %s', port)
            return False
        finally:
            if ser is not None:
                ser.close()

    def _check_tmc(self, fn, cmd_id, re_resp):
        fd = None
        try:
            fd = os.open('/dev/' + fn, os.O_RDWR)
            os.write(fd, cmd_id)
            resp = os.read(fd, 1000)
            self.logger.debug('read %s', repr(resp))
            if re_resp.match(resp) is not None:
                tmcid = int(DetectUSB.re_USBTMC.match(fn).groupdict()['tmcid'])
                return tmcid
        except Exception:
            self.logger.exception('_check_tmc %s', fn)
            return None
        finally:
            if fd is not None:
                os.close(fd)


class ESS(object):
    """ESS process implementation"""

    def __init__(self, jsfn, jsdata=None):
        """ Constructor.
jsfn - file name of JSON config file
jsdata - JSON data (str), ignored if jsfn is not None"""
        # clear conditional members to None
        self.ps = None
        self.bme = None
        self.flir = None
        self.chamber = None
        self.td = None
        self.afg = None
        self.trigger = None
        self.pc = None
        self.splitmode = None
        self.spliton = None
        self.splitgain = None
        self.starttime = None
        self.essprog = None
        self.db = None
        self.grafana = None
        self.ed = None

        if jsfn is not None:
            with open(jsfn, 'r') as fp:
                d = json.load(fp)
            configfn = jsfn
        elif jsdata is not None:
            d = json.loads(jsdata)
            configfn = None
        else:
            raise ValueError("No JSON config provided")

        self.phase = d['phase']
        self.tester = d['tester']

        # basetime
        dt = datetime.now()
        dt = dt.replace(second=0, microsecond=0) + timedelta(seconds=60)

        # datadir
        self.datadir = dt.strftime(
            d.get('datadir', 'data-%Y%m%d/'))
        if self.datadir[-1] != os.sep:
            self.datadir += os.sep
        if not os.path.isdir(self.datadir):
            os.mkdir(self.datadir)
        # save configuration
        if configfn is not None:
            shutil.copy(configfn, self.datadir)
        else:
            with open(self.datadir + 'config.json', 'w') as fp:
                fp.write(jsdata)

        if 'comment' in d:
            with open(self.datadir + 'README.txt', 'w') as f:
                f.write(d['comment'] + '\n')

        if 'logging' in d:
            kwargs = {key: d['logging'][key]
                      for key in ('level', 'format', 'filename')
                      if key in d['logging']}
            if 'filename' in kwargs:
                kwargs['filename'] = dt.strftime(kwargs['filename'])
                if kwargs['filename'][0] not in ('.', os.sep):
                    kwargs['filename'] = self.datadir + kwargs['filename']
            logging.basicConfig(**kwargs)

        # queues
        self.q_ndata = multiprocessing.JoinableQueue()
        self.q_dpres = multiprocessing.Queue()
        self.q_log = multiprocessing.Queue()
        self.q_resp = queue.Queue()
        self.q_att = queue.Queue()
        # manager for shared dict for invalid channels
        self.mgr = multiprocessing.Manager()
        self.invalid_chs_dict = self.mgr.dict()

        self.qlistener = logging.handlers.QueueListener(
            self.q_log, QLogHandler())
        self.qlistener.start()

        # UUB channels
        self.chans = d.get('chans', range(1, 11))

        # start DataProcessors before anything is logged, otherwise child
        # processes may lock at acquiring lock to existing log handlers
        dp_ctx = {'q_ndata': self.q_ndata,
                  'q_resp': self.q_dpres,
                  'q_log': self.q_log,
                  'inv_chs_dict': self.invalid_chs_dict,
                  'datadir': self.datadir,
                  'splitmode': d.get('splitmode', None),
                  'chans': self.chans}
        if 'afg' in d:
            afgkwargs = d["afg"]
            for key in ('hswidth', 'Pvoltage', 'Fvoltage', 'freq'):
                dp_ctx[key] = afgkwargs.get(key, AFG.PARAM[key])
        else:
            afgkwargs = {}
        self.n_dp = d.get('n_dp', multiprocessing.cpu_count() - 2)
        self.dataprocs = [multiprocessing.Process(
            target=DataProcessor, name='DP%d' % i, args=(dp_ctx, ))
                          for i in range(self.n_dp)]
        for dp in self.dataprocs:
            dp.start()
        self.qdispatch = QueDispatch(self.q_dpres, self.q_resp, zLog=False)
        self.qdispatch.start()

        # detect USB
        # ports has priority over detected devices
        if 'devlist' in d:
            du = DetectUSB()
            found = du.detect(d['devlist'])
            if 'power_cpx' in found and 'power_hmp' in found:
                if 'powerdev' in d:
                    assert d['powerdev'] in ('power_hmp', 'power_cpx')
                    found['power'] = found.pop(d['powerdev'])
                else:
                    raise RuntimeError(
                        'Both power_hmp and power_cpx detected, ' +
                        'cannot distinguish which should be used')
            elif 'power_cpx' in found:
                found['power'] = found.pop('power_cpx')
            elif 'power_hmp' in found:
                found['power'] = found.pop('power_hmp')
            if 'ports' in d:
                found.update(d['ports'])
            d['ports'] = found
            del du

        # timer
        self.basetime = dt
        self.timer = Timer(dt)
        # event to stop
        self.timer.timerstop = self.timerstop = threading.Event()
        self.timer.start()
        if d.get('evtdisp', False):
            self.ed = EvtDisp(self.timer)
            self.ed.start()

        assert len(d['uubnums']) <= 10 and \
            all([isinstance(uubnum, int) and 0 <= uubnum <= VIRGINUUBNUM
                 for uubnum in d['uubnums'] if uubnum is not None])
        self.uubnums = d['uubnums']
        # None filtered out
        luubnums = [uubnum for uubnum in self.uubnums if uubnum is not None]

        # DB connector
        self.dbcon = DBconnector(self, d['dbinfo'], 'db' in d['dataloggers'])
        self.internalSNs = self.dbcon.queryInternalSN()

        # power supply
        if 'power' in d and 'power' in d['ports']:
            port = d['ports']['power']
            self.ps = PowerSupply(port, self.timer, self.q_resp, **d['power'])
            self.ps.start()

        # BME
        if 'BME' in d['ports']:
            port = d['ports']['BME']
            self.bme = BME(port, self.timer, self.q_resp)
            self.bme.start()

        # FLIR
        if 'flir' in d['ports']:
            port = d['ports']['flir']
            uubnum = d.get('flir.uubnum', 0)
            imtype = str(d['flir.imtype']) if 'flir.imtype' in d else None
            self.flir = FLIR(port, self.timer, self.q_att, self.datadir,
                             uubnum, imtype)
            self.flir.start()

        # chamber
        if 'chamber' in d['ports']:
            port = d['ports']['chamber']
            self.chamber = Chamber(port, self.timer, self.q_resp)
            self.chamber.start()

        # TrigDelay
        if 'trigdelay' in d['ports']:
            predefined = d.get('trigdelay', None)
            self.td = TrigDelay(d['ports']['trigdelay'], predefined)

        # AFG
        if 'afg' in d:
            self.afg = AFG(**afgkwargs)

        # Trigger
        if 'trigger' in d:
            trigger = d['trigger']
            assert trigger in ('RPi', 'TrigDelay', 'AFG'), \
                "Unknown trigger %s" % trigger
            if trigger == 'RPi':
                self.trigger = RPiTrigger().trigger
            elif trigger == 'TrigDelay':
                assert self.td is not None, \
                    "TrigDelay as trigger required, but it does not exist"
                self.trigger = self.td.trigger
            elif trigger == 'AFG':
                assert self.afg is not None, \
                    "AFG as trigger required, but it does not exist"
                self.trigger = self.afg.trigger

        # PowerControl
        if 'powercontrol' in d['ports']:
            self.pc = PowerControl(d['ports']['powercontrol'], self,
                                   dp_ctx['splitmode'])
            if 'pc_limits' in d:
                self.pc.setCurrLimits(d['pc_limits'], True)
            self.splitmode = self.pc._set_splitterMode
            self.spliton = self.pc.splitterOn
            self.pc.start()

        # SplitterGain & notcalc
        if self.afg is not None:
            if 'splitter' in d:
                calibration = d['splitter'].get('calibration', None)
                self.splitgain = SplitterGain(self.afg.param['gains'], None,
                                              self.uubnums, calibration)
            else:
                self.splitgain = DirectGain()
            self.notcalc = make_notcalc(dp_ctx)

        # UUBs - UUBdaq and UUBlisten
        self.ulisten = UUBlisten(self.q_ndata)
        self.ulisten.start()
        self.udaq = UUBdaq(self.timer, self.ulisten, self.q_resp, self.q_ndata,
                           self.afg, self.splitmode, self.spliton,
                           self.td, self.trigger)
        self.udaq.start()
        self.udaq.uubnums2add.extend(luubnums)

        # UUBs - UUBtelnet
        dloadfn = d.get('download_fn', None)
        if dloadfn is not None and dloadfn[0] not in ('.', os.sep):
            dloadfn = self.datadir + dloadfn
        self.telnet = UUBtelnet(self.timer, luubnums, dloadfn)
        self.telnet.start()

        # UUBs - Zync temperature & SlowControl
        self.uubtsc = {uubnum: UUBtsc(uubnum, self.timer, self.q_resp)
                       for uubnum in luubnums}
        for uub in self.uubtsc.values():
            uub.start()

        # tickers
        if 'meas.thp' in d['tickers']:
            thp_period = d['tickers'].get('meas.thp', 30)
            self.timer.add_ticker('meas.thp', periodic_ticker(thp_period))
        if 'meas.sc' in d['tickers']:
            sc_period = d['tickers'].get('meas.sc', 30)
            self.timer.add_ticker('meas.sc', periodic_ticker(sc_period))

        # ESS program
        if 'essprogram' in d['tickers']:
            fn = d['tickers']['essprogram']
            if 'essprogram.macros' in d['tickers']:
                dm = d['tickers']['essprogram.macros']
                essprog_macros = {key: str(val) for key, val in dm.items()}
            else:
                essprog_macros = None
            with open(fn, 'r') as fp:
                self.essprog = ESSprogram(fp, self.timer, self.q_resp,
                                          essprog_macros)
            shutil.copy(fn, self.datadir)
            self.essprog.start()
            if 'startprog' in d['tickers']:
                self.essprog.startprog(int(d['tickers']['startprog']))
                self.starttime = self.essprog.starttime
                self.dbcon.start()

        #  ===== DataLogger & handlers =====
        self.dl = DataLogger(self.q_resp)
        dpfilter_linear = None
        dpfilter_cutoff = None
        dpfilter_ramp = None
        dpfilter_stat_pede = None
        dpfilter_stat_noise = None
        # temperature
        if d['dataloggers'].get('temperature', False):
            self.dl.add_handler(
                makeDLtemperature(self, luubnums, 'meas.sc' in d['tickers']))

        # slow control measured values
        if d['dataloggers'].get('slowcontrol', False):
            for uubnum in luubnums:
                self.dl.add_handler(makeDLslowcontrol(self, uubnum),
                                    uubnum=uubnum)

        # currents measured by power supply and power control
        if d['dataloggers'].get('currents', False):
            self.dl.add_handler(makeDLcurrents(self, luubnums))

        # pedestals & their std
        if d['dataloggers'].get('pede', False):
            count = d['dataloggers'].get('pedestatcount', None)
            for uubnum in luubnums:
                self.dl.add_handler(makeDLpedenoise(self, uubnum, count),
                                    uubnum=uubnum)
            if count is not None:
                if dpfilter_stat_pede is None:
                    dpfilter_stat_pede = make_DPfilter_stat('pede')
                if dpfilter_stat_noise is None:
                    dpfilter_stat_noise = make_DPfilter_stat('noise')
                for uubnum in luubnums:
                    self.dl.add_handler(makeDLstat(self, uubnum, 'pede'),
                                        (dpfilter_stat_pede, ), uubnum)
                    self.dl.add_handler(makeDLstat(self, uubnum, 'noise'),
                                        (dpfilter_stat_noise, ), uubnum)

        # amplitudes of halfsines
        if 'ampli' in d['dataloggers']:
            for uubnum in luubnums:
                self.dl.add_handler(makeDLhsampli(
                    self, uubnum, d['dataloggers']['ampli']), uubnum=uubnum)

        # amplitudes of sines vs freq
        if 'fampli' in d['dataloggers']:
            for uubnum in luubnums:
                self.dl.add_handler(makeDLfampli(
                    self, uubnum, d['dataloggers']['fampli']), uubnum=uubnum)

        # gain/linearity
        if d['dataloggers'].get('linearity', False):
            if dpfilter_linear is None:
                dpfilter_linear = make_DPfilter_linear(
                    self.notcalc, self.splitgain)
            for uubnum in luubnums:
                self.dl.add_handler(makeDLlinear(self, uubnum),
                                    (dpfilter_linear, ), uubnum)

        # freqgain
        if 'freqgain' in d['dataloggers']:
            if dpfilter_linear is None:
                dpfilter_linear = make_DPfilter_linear(
                    self.notcalc, self.splitgain)
            freqs = d['dataloggers']['freqgain']
            for uubnum in luubnums:
                self.dl.add_handler(makeDLfreqgain(self, uubnum, freqs),
                                    (dpfilter_linear, ), uubnum)

        # cut-off
        if d['dataloggers'].get('cutoff', False):
            if dpfilter_linear is None:
                dpfilter_linear = make_DPfilter_linear(
                    self.notcalc, self.splitgain)
            if dpfilter_cutoff is None:
                dpfilter_cutoff = make_DPfilter_cutoff()
            for uubnum in luubnums:
                self.dl.add_handler(makeDLfreqgain(self, uubnum, ),
                                    (dpfilter_linear, dpfilter_cutoff), uubnum)

        # ramp
        if d['dataloggers'].get('ramp', False):
            if dpfilter_ramp is None:
                dpfilter_ramp = make_DPfilter_ramp(luubnums)
            fn = self.datadir + self.basetime.strftime('ramp-%Y%m%d.log')
            lh = LogHandlerRamp(fn, self.basetime, luubnums)
            self.dl.add_handler(lh, (dpfilter_ramp, ))

        # power on/off - voltage ramp
        if d['dataloggers'].get('voltramp', False):
            fn = self.datadir + self.basetime.strftime('voltramp-%Y%m%d.log')
            self.dl.add_handler(LogHandlerVoltramp(fn, self.basetime,
                                                   luubnums))

        # database
        if 'db' in d['dataloggers']:
            flabels = d['dataloggers']['db'].get('flabels', None)
            for item in d['dataloggers']['db']['logitems']:
                if item == 'ramp':
                    if dpfilter_ramp is None:
                        dpfilter_ramp = make_DPfilter_ramp(luubnums)
                    self.dl.add_handler(self.dbcon.getLogHandler(item),
                                        (dpfilter_ramp, ))
                elif item == 'cutoff':
                    if dpfilter_linear is None:
                        dpfilter_linear = make_DPfilter_linear(
                            self.notcalc, self.splitgain)
                    if dpfilter_cutoff is None:
                        dpfilter_cutoff = make_DPfilter_cutoff()
                    self.dl.add_handler(self.dbcon.getLogHandler(item),
                                        (dpfilter_linear, dpfilter_cutoff))
                elif item in ('gain', 'freqgain'):
                    if dpfilter_linear is None:
                        dpfilter_linear = make_DPfilter_linear(
                            self.notcalc, self.splitgain)
                    self.dl.add_handler(
                        self.dbcon.getLogHandler(item, flabels=flabels),
                        (dpfilter_linear, ))
                elif item == 'noisestat':
                    if dpfilter_stat_pede is None:
                        dpfilter_stat_pede = make_DPfilter_stat('pede')
                    if dpfilter_stat_noise is None:
                        dpfilter_stat_noise = make_DPfilter_stat('noise')
                    self.dl.add_handler(self.dbcon.getLogHandler(item),
                                        (dpfilter_stat_pede,
                                         dpfilter_stat_noise))
                else:
                    self.dl.add_handler(self.dbcon.getLogHandler(item))

        # grafana: filters must be already created before
        if 'grafana' in d['dataloggers']:
            lh = LogHandlerGrafana(
                self.starttime, luubnums, d['dataloggers']['grafana'])
            self.dl.add_handler(
                lh, (dpfilter_stat_pede, dpfilter_stat_noise, dpfilter_linear))

        # pickle: filters must be already created before
        if d['dataloggers'].get('pickle', False):
            fn = self.datadir + dt.strftime('pickle-%Y%m%d')
            lh = LogHandlerPickle(fn)
            self.dl.add_handler(
                lh, (dpfilter_stat_pede, dpfilter_stat_noise, dpfilter_linear))

        self.dl.start()

    def removeUUB(self, uubnum):
        """Remove UUB from running system"""
        # self.logger.debug('Removing UUB #%04d', uubnum)
        ind = self.uubnums.index(uubnum)
        self.uubnums[ind] = None
        if self.pc is not None:
            self.pc.uubnums2del.append(uubnum)
        self.udaq.uubnums2del.append(uubnum)
        self.telnet.uubnums2del.append(uubnum)
        self.dl.uubnums2del.append(uubnum)
        uub = self.uubtsc.pop(uubnum)
        uub.stopme = True
        # self.logger.debug('Joining UUBtsc #%04d', uubnum)
        uub.join()
        # self.logger.debug('UUB #%04d removed', uubnum)

    def __del__(self):
        self.stop()

    def _noaction(self):
        pass

    def stop(self):
        """Stop all threads and processes"""
        self.timer.stop.set()
        self.timer.evt.set()
        self.dl.stop.set()
        self.dbcon.close()
        self.ulisten.stop.set()
        for i in range(self.n_dp):
            self.q_ndata.put(None)
        if self.q_dpres is not None:
            self.q_dpres.put(None)
        if self.afg is not None:
            self.afg.stop()
        if self.td is not None:
            self.td.stop()
        if self.pc is not None:
            self.pc.switch(False, True)  # switch off all relays
            self.pc.stop()
        # stop RPiTrigger
        if self.trigger is not None and \
           isinstance(self.trigger.__self__, RPiTrigger):
            self.trigger.__self__.stop()
        # join all threads
        self.timer.join()
        self.qlistener.stop()
        if self.ps is not None:
            self.ps.join()
        if self.bme is not None:
            self.bme.join()
        if self.flir is not None:
            self.flir.join()
        if self.chamber is not None and self.chamber.binder is not None:
            self.chamber.binder.setState(Binder.STATE_BASIC)
            self.chamber.join()
        self.ulisten.join()
        self.udaq.join()
        self.telnet.join()
        for uub in self.uubtsc.values():
            uub.join()
        self.qdispatch.join()
        if self.essprog is not None:
            self.essprog.join()
        self.dl.join()
        # join DP processes
        for dp in self.dataprocs:
            dp.join()
        self.mgr.shutdown()
        self.stop = self._noaction
        print("ESS.stop() finished")


def Pretest(jsfn, uubnum):
    """Wrap for ESS with uubnum"""
    subst = {'UUBNUM': uubnum,
             'UUBNSTR': '%04d' % uubnum,
             'MACADDR': uubnum2mac(uubnum)}
    with open(jsfn, 'r') as fp:
        js = fp.read()
    for key, val in subst.items():
        js = js.replace('$'+key, str(val))
    return ESS(jsfn=None, jsdata=js)


if __name__ == '__main__':
    try:
        ess = ESS(sys.argv[1])
    except (IndexError, IOError, ValueError):
        print("Usage: %s <JSON config file>" % sys.argv[0])
        raise

    logger = logging.getLogger('ESS')
    # start RPyC TBD
    logger.info('ESSprogram started, waiting for timerstop.')
    ess.timerstop.wait()
    logger.info('Stopping everything.')
    ess.stop()
    if ess.dbcon.files is not None:
        ess.dbcon.commit()
    logger.info('Done. Everything stopped.')
