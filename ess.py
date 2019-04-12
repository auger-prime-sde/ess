"""
 ESS procedure
 main program
"""

import os
import sys
import json
import logging
import threading
from datetime import datetime, timedelta
from Queue import Queue
from prc import PRCServer

# ESS stuff
from timer import Timer, periodic_ticker, one_tick
from logger import LogHandlerFile, LogHandlerRamp, LogHandlerPickle, DataLogger
from logger import makeDLtemperature, makeDLslowcontrol, makeDLpedestals
from logger import makeDLhsampli, makeDLfampli, makeDLlinear
from logger import makeDLfreqgain, makeDLcutoff
from logger import QuePipeView
from BME import BME, TrigDelay, PowerControl
from UUB import UUBdaq, UUBlisten, UUBconvData, UUBtelnet, UUBtsc
from UUB import uubnum2mac
from chamber import Chamber, ESSprogram
from dataproc import DataProcessor, item2label
from dataproc import DP_store, DP_freq, DP_pede, DP_hsampli, DP_ramp
from dataproc import make_DPfilter_linear, make_DPfilter_ramp
from dataproc import make_DPfilter_cutoff
from afg import AFG, RPiTrigger
from power import PowerSupply

VERSION = '20190408'


class ESS(object):
    """ESS process implementation"""

    def __init__(self, js):
        if hasattr(js, 'read'):
            d = json.load(js)
        else:
            d = json.loads(js)

        # basetime
        dt = datetime.now()
        dt = dt.replace(second=0, microsecond=0) + timedelta(seconds=60)

        # event to stop
        self.evtstop = threading.Event()
        self.prcport = d['ports'].get('prc', None)

        # datadir
        self.datadir = dt.strftime(
            d.get('datadir', 'data-%Y%m%d/'))
        if self.datadir[-1] != '/':
            self.datadir += '/'
        if not os.path.isdir(self.datadir):
            os.mkdir(self.datadir)

        if 'comment' in d:
            with open(self.datadir + 'README.txt', 'w') as f:
                f.write(d['comment'] + '\n')

        if 'logging' in d:
            kwargs = {key: d['logging'][key]
                      for key in ('level', 'format', 'filename')
                      if key in d['logging']}
            if 'filename' in kwargs:
                kwargs['filename'] = dt.strftime(kwargs['filename'])
                if kwargs['filename'][0] not in ('.', '/'):
                    kwargs['filename'] = self.datadir + kwargs['filename']
            logging.basicConfig(**kwargs)

        self.basetime = dt
        self.timer = Timer(dt)
        self.timer.start()

        # queues
        self.q_resp = Queue()
        self.q_ndata = Queue()
        self.q_dp = Queue()

        # UUB channels
        self.lowgains = d.get('lowgains', [1, 3, 5, 7, 9])
        self.highgains = d.get('highgains', [2, 4, 6, 10])
        self.chans = sorted(self.lowgains+self.highgains)

        # power supply
        if 'power' in d and 'power' in d['ports']:
            port = d['ports']['power']
            self.ps = PowerSupply(port, self.timer, **d['power'])
            self.ps.start()

        # BME
        if 'BME' in d['ports']:
            port = d['ports']['BME']
            self.bme = BME(port, self.timer, self.q_resp)
            self.bme.start()

        # chamber
        if 'chamber' in d['ports']:
            port = d['ports']['chamber']
            self.chamber = Chamber(port, self.timer, self.q_resp)
            self.chamber.start()

        # AFG & RPi trigger
        self.afg = None
        self.trigger = None
        if 'afg' in d:
            kwargs = d.get("afg", {})
            self.afg = AFG(**kwargs)
            self.trigger = self.afg.trigger
        if d.get('RPiTrigger', False):
            self.trigger = RPiTrigger().trigger

        # TrigDelay
        if 'trigdelay' in d['ports']:
            predefined = d.get('trigdelay', None)
            self.td = TrigDelay(d['ports']['trigdelay'], predefined)
        else:
            self.td = None

        self.uubnums = [int(uubnum) for uubnum in d['uubnums']]
        # PowerControl
        self.splitmode = None
        if 'powercontrol' in d['ports']:
            self.pc = PowerControl(d['ports']['powercontrol'], self.timer,
                                   self.q_resp, self.uubnums)
            self.splitmode = self.pc.splitterMode

        # UUBs - UUBdaq and UUBlisten
        self.ulisten = UUBlisten(self.q_ndata)
        self.ulisten.start()
        self.uconv = UUBconvData(self.q_ndata, self.q_dp)
        self.uconv.start()
        self.udaq = UUBdaq(self.timer, self.ulisten, self.q_resp,
                           self.afg, self.splitmode, self.td,
                           self.trigger)
        self.udaq.start()
        self.udaq.uubnums2add.extend(self.uubnums)

        # UUBs - UUBtelnet
        self.telnet = UUBtelnet(self.timer, *self.uubnums)
        self.telnet.start()

        # UUBs - Zync temperature & SlowControl
        self.uubtsc = {uubnum: UUBtsc(uubnum, self.timer, self.q_resp)
                       for uubnum in self.uubnums}
        for uub in self.uubtsc.itervalues():
            uub.start()

        # data processing
        self.dp0 = DataProcessor(self.q_dp)
        self.dp0.workhorses.append(DP_ramp(self.q_resp))
        self.dp0.workhorses.append(DP_pede(self.q_resp))
        if self.afg is not None:
            self.dp0.workhorses.append(DP_hsampli(
                self.q_resp, self.afg.param['hswidth'],
                self.lowgains, self.chans))
            self.dp0.workhorses.append(DP_freq(
                self.q_resp, self.lowgains, self.chans))
        self.dp0.workhorses.append(DP_store(self.datadir))
        self.dp0.start()

        # tickers
        if 'meas.thp' in d['tickers']:
            thp_period = d['tickers'].get('meas.thp', 30)
            self.timer.add_ticker('meas.thp', periodic_ticker(thp_period))
        if 'meas.sc' in d['tickers']:
            sc_period = d['tickers'].get('meas.sc', 30)
            self.timer.add_ticker('meas.sc', periodic_ticker(sc_period))
        if 'meas.iv' in d['tickers']:
            iv_period = d['tickers'].get('meas.iv', 30)
            self.timer.add_ticker('meas.iv', periodic_ticker(iv_period))

        # ESS program
        self.starttime = None
        if 'essprogram' in d['tickers']:
            fn = d['tickers']['essprogram']
            essprog_macros = d['tickers'].get('essprogram.macros', None)
            with open(fn, 'r') as fp:
                self.essprog = ESSprogram(fp, self.timer, self.q_resp,
                                          essprog_macros)
            self.essprog.start()
            if 'startprog' in d['tickers']:
                self.essprog.startprog(int(d['tickers']['startprog']))
                self.starttime = self.essprog.starttime

        #  ===== DataLogger & handlers =====
        DEBUG_Q_RESP = True
        if DEBUG_Q_RESP:
            q_resp_out = Queue()
            qpv = QuePipeView(self.timer, self.q_resp, q_resp_out)
            qpv.start()
        else:
            q_resp_out = self.q_resp
        self.dl = DataLogger(q_resp_out)
        dpfilter_linear = None
        dpfilter_cutoff = None
        dpfilter_ramp = None
        # temperature
        if d['dataloggers'].get('temperature', False):
            self.dl.add_handler(
                makeDLtemperature(self, self.uubnums,
                                  'meas.sc' in d['tickers']))

        # slow control measured values
        if d['dataloggers'].get('slowcontrol', False):
            for uubnum in self.uubnums:
                self.dl.add_handler(makeDLslowcontrol(self, uubnum))

        # pedestals & their std
        if d['dataloggers'].get('pede', False):
            for uubnum in self.uubnums:
                self.dl.add_handler(makeDLpedestals(self, uubnum))

        # amplitudes of halfsines
        if 'ampli' in d['dataloggers']:
            for uubnum in self.uubnums:
                self.dl.add_handler(makeDLhsampli(
                    self, uubnum, d['dataloggers']['ampli']))

        # amplitudes of sines vs freq
        if 'fampli' in d['dataloggers']:
            for uubnum in self.uubnums:
                self.dl.add_handler(makeDLfampli(
                    self, uubnum, d['dataloggers']['fampli']))

        # gain/linearity
        if d['dataloggers'].get('linearity', False):
            if dpfilter_linear is None:
                dpfilter_linear = make_DPfilter_linear(self.lowgains,
                                                       self.highgains)
            for uubnum in self.uubnums:
                self.dl.add_handler(makeDLlinear(self, uubnum),
                                    (dpfilter_linear, ))

        # freqgain
        if 'freqgain' in d['dataloggers']:
            if dpfilter_linear is None:
                dpfilter_linear = make_DPfilter_linear(self.lowgains,
                                                       self.highgains)
            freqs = d['dataloggers']['freqgain']
            for uubnum in self.uubnums:
                self.dl.add_handler(makeDLfreqgain(self, uubnum, ),
                                    (dpfilter_linear, ))

        # cut-off
        if d['dataloggers'].get('freqgain', False):
            if dpfilter_linear is None:
                dpfilter_linear = make_DPfilter_linear(self.lowgains,
                                                       self.highgains)
            if dpfilter_cutoff is None:
                dpfilter_cutoff = make_DPfilter_cutoff(self.lowgains,
                                                       self.highgains)
            for uubnum in self.uubnums:
                self.dl.add_handler(makeDLfreqgain(self, uubnum, ),
                                    (dpfilter_linear, dpfilter_cutoff))

        # ramp
        if d['dataloggers'].get('ramp', False):
            if dpfilter_ramp is None:
                dpfilter_ramp = make_DPfilter_ramp(self.uubnums)
            fn = self.datadir + self.basetime.strftime('ramp-%Y%m%d.log')
            lh = LogHandlerRamp(fn, self.basetime)
            self.dl.add_handler(lh, (dpfilter_ramp, ))

        # database
        if 'db' in d['dataloggers']:
            self.db = DBconnector(self, d['dataloggers']['db'])
            for item in d['dataloggers']['db']['logitems']:
                if item == 'ramp':
                    if dpfilter_ramp is None:
                        dpfilter_ramp = make_DPfilter_ramp(self.uubnums)
                    self.dl.add_handler(self.db.getLogHandler(item),
                                        (dpfilter_ramp, ))
                elif item == 'cutoff':
                    if dpfilter_linear is None:
                        dpfilter_linear = make_DPfilter_linear(
                            self.lowgains, self.highgains)
                    if dpfilter_cutoff is None:
                        dpfilter_cutoff = make_DPfilter_cutoff(
                            self.lowgains, self.highgains)
                    self.dl.add_handler(self.db.getLogHandler(item),
                                        (dpfilter_linear, dpfilter_cutoff))
                elif item in ('gain', 'freqgain'):
                    if dpfilter_linear is None:
                        dpfilter_linear = make_DPfilter_linear(self.lowgains,
                                                               self.highgains)
                    self.dl.add_handler(self.db.getLogHandler(item),
                                        (dpfilter_linear, ))
                else:
                    self.dl.add_handler(self.db.getLogHandler(item))

        # pickle
        if d['dataloggers'].get('pickle', False):
            fn = self.datadir + dt.strftime('pickle-%Y%m%d')
            lh = LogHandlerPickle(fn)
            self.dl.add_handler(lh)

        self.dl.start()

    def stop(self):
        """Stop all threads"""
        self.timer.stop.set()
        self.timer.evt.set()
        self.dl.stop.set()
        self.dp0.stop.set()
        self.ulisten.stop.set()
        self.uconv.stop.set()


def Pretest(jsconf, uubnum):
    """Wrap for ESS with uubnum"""
    subst = {'UUBNUM': uubnum,
             'MACADDR': uubnum2mac(uubnum)}
    with open(jsconf, 'r') as fp:
        js = fp.read()
    for key, val in subst.iteritems():
        js = js.replace('$'+key, str(val))
    return js


if __name__ == '__main__':
    try:
        with open(sys.argv[1], 'r') as fp:
            ess = ESS(fp)
    except (IndexError, IOError, ValueError):
        print("Usage: %s <JSON config file>" % sys.argv[0])
        raise

    logger = logging.getLogger('ESS')
    if ess.prcport is not None:
        logger.info('Starting PRC server at localhost:%d', ess.prcport)
        server = PRCServer(ip='127.0.0.1', port=ess.prcport)
        server.add_variable('ess', ess)
        server.start()
        print 'PRC server started at localhost:%d' % ess.prcport

    logger.debug('Waiting for ess.evtstop.')
    ess.evtstop.wait()
    logger.info('Stopping everything.')
    ess.stop()
    logger.info('Everything stopped.')
