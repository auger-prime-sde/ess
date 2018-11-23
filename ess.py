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
from logger import LogHandlerFile, LogHandlerPickle, DataLogger
from BME import BME, TrigDelay
from UUB import UUBdaq, UUBlisten, UUBconvData, UUBtelnet, UUBtsc
from chamber import Chamber, ESSprogram
from dataproc import DataProcessor, item2label, DP_pede, DP_hsampli
from dataproc import DP_store, DP_freq, dpfilter_linear
from afg import AFG
from power import PowerSupply

VERSION = '20181022'


class ESS(object):
    """ESS process implementation"""

    def __init__(self, js):
        if hasattr(js, 'read'):
            d = json.load(js)
        else:
            d = json.loads(js)

        # event to stop
        self.evtstop = threading.Event()
        self.prcport = d['ports'].get('prc', None)

        # datadir
        self.datadir = datetime.now().strftime(
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
                kwargs['filename'] = datetime.now().strftime(
                    kwargs['filename'])
                if kwargs['filename'][0] not in ('.', '/'):
                    kwargs['filename'] = self.datadir + kwargs['filename']
            logging.basicConfig(**kwargs)

        dt = datetime.now()
        dt = dt.replace(second=0, microsecond=0) + timedelta(seconds=60)
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

        # AFG
        kwargs = d.get("afg", {})
        self.afg = AFG(**kwargs)

        # TrigDelay
        if 'trigdelay' in d['ports']:
            kwargs = d.get('trigdelay', {})
            self.td = TrigDelay(d['ports']['trigdelay'], kwargs)

        self.uubnums = [int(uubnum) for uubnum in d['uubnums']]
        # UUBs - UUBdaq and UUBlisten
        self.ulisten = UUBlisten(self.q_ndata)
        self.ulisten.start()
        self.uconv = UUBconvData(self.q_ndata, self.q_dp)
        self.uconv.start()
        self.udaq = UUBdaq(self.timer, self.afg, self.ulisten, self.td,
                           self.q_resp)
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
        self.dp0.workhorses.append(DP_pede(self.q_resp))
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
        if 'essprogram' in d['tickers']:
            fn = d['tickers']['essprogram']
            with open(fn, 'r') as fp:
                self.essprog = ESSprogram(fp, self.timer, self.q_resp)
            self.essprog.start()
            if 'startprog' in d['tickers']:
                self.essprog.startprog(int(d['tickers']['startprog']))

        #  ===== DataLogger & handlers =====
        # handler for amplitudes

        self.dl = DataLogger(self.q_resp)
        # temperature
        if d['dataloggers'].get('temperature', False):
            prolog = """\
# Temperature measurement: BME + chamber + Zynq
# date %s
# columns: timestamp | set.temp | BME1.temp | BME2.temp | chamber.temp""" % (
                dt.strftime('%Y-%m-%d'))
            prolog += ''.join([' | UUB-%04d.zynq_temp' % uubnum
                               for uubnum in self.uubnums])
            if 'meas.sc' in d['tickers']:
                prolog += ''.join([' | UUB-%04d.sc_temp' % uubnum
                                   for uubnum in self.uubnums])
            prolog += '\n'
            logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                       '{set_temp:6.1f}',
                       '{bme_temp1:7.2f}',
                       '{bme_temp2:7.2f}',
                       '{chamber_temp:7.2f}']
            logdata += ['{zynq%04d_temp:5.1f}' % uubnum
                        for uubnum in self.uubnums]
            if 'meas.sc' in d['tickers']:
                logdata += ['{sc%04d_temp:5.1f}' % uubnum
                            for uubnum in self.uubnums]
            formatstr = ' '.join(logdata) + '\n'
            fn = self.datadir + dt.strftime('thp-%Y%m%d.log')
            self.dl.handlers.append(LogHandlerFile(
                fn, formatstr, prolog=prolog))

        # slow control measured values
        if d['dataloggers'].get('slowcontrol', False):
            labels_I = ('1V', '1V2', '1V8', '3V3', '3V3_sc', 'P3V3', 'N3V3',
                        '5V', 'radio', 'PMTs')
            labels_U = ('1V', '1V2', '1V8', '3V3', 'P3V3', 'N3V3',
                        '5V', 'radio', 'PMTs', 'ext1', 'ext2')
            for uubnum in self.uubnums:
                fn = self.datadir + ('sc_uub%04d' % uubnum) +\
                     dt.strftime('-%Y%m%d.log')
                prolog = """\
# Slow Control measured values
# UUB #%04d, date %s
# voltages in mV, currents in mA
# columns: timestamp""" % (uubnum, dt.strftime('%Y-%m-%d'))
                logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}']
                prolog += ''.join([' | I_%s' % label for label in labels_I])
                logdata.extend(['{sc%04d_i_%s:5.2f}' % (uubnum, label)
                                for label in labels_I])
                prolog += ''.join([' | U_%s' % label for label in labels_U])
                logdata.extend(['{sc%04d_u_%s:7.2f}' % (uubnum, label)
                                for label in labels_U])
                prolog += '\n'
                formatstr = ' '.join(logdata) + '\n'
                lh = LogHandlerFile(fn, formatstr, prolog=prolog)
                self.dl.handlers.append(lh)

        # pedestals & their std
        if 'pede' in d['dataloggers']:
            item = {key: d['dataloggers']['pede'][key]
                    for key in ('voltage', 'ch2')}
            item['functype'] = 'P'
            for uubnum in self.uubnums:
                fn = self.datadir + ('pede_uub%04d' % uubnum) +\
                     dt.strftime('-%Y%m%d.log')
                prolog = """\
# Pedestals and their std. dev.
# UUB #%04d, date %s
# columns: timestamp | meas_pulse_point""" % (uubnum, dt.strftime('%Y-%m-%d'))
                logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                           '{meas_pulse_point:2d}']
                for typ, fmt in (('pede', '7.2f'), ('pedesig', '7.2f')):
                    prolog += ''.join([' | %s.ch%d' % (typ, chan)
                                       for chan in self.chans])
                    logdata += ['{%s:%s}' % (item2label(item, uubnum=uubnum,
                                                        chan=chan, typ=typ),
                                             fmt)
                                for chan in self.chans]
                prolog += '\n'
                formatstr = ' '.join(logdata) + '\n'
                lh = LogHandlerFile(
                    fn, formatstr, prolog=prolog,
                    skiprec=lambda d: 'meas_pulse_point' not in d)
                self.dl.handlers.append(lh)

        # amplitudes of halfsines
        if 'ampli' in d['dataloggers']:
            voltages, ch2s = (d['dataloggers']['ampli'][key]
                              for key in ('voltages', 'ch2s'))
            item = {'functype': 'P', 'typ': 'ampli'}
            for uubnum in self.uubnums:
                fn = self.datadir + ('ampli_uub%04d' % uubnum) +\
                     dt.strftime('-%Y%m%d.log')
                prolog = """\
# Amplitudes of halfsines
# UUB #%04d, date %s
# columns: timestamp | meas_pulse_point | ch2 | voltage | """ % (
                    uubnum, dt.strftime('%Y-%m-%d'))
                prolog += ' | '.join(['ampli.ch%d' % chan
                                      for chan in self.chans])
                prolog += '\n'
                loglines = []
                for ch2 in ch2s:
                    for voltage in voltages:
                        logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                                   '{meas_pulse_point:2d}',
                                   '%d %.1f' % (ch2, voltage)]
                        logdata += [' '*7 if (chan in self.highgains and ch2)
                                    else '{%s:7.2f}' % item2label(
                                            item, uubnum=uubnum, chan=chan,
                                            voltage=voltage, ch2=ch2)
                                    for chan in self.chans]
                        loglines.append(' '.join(logdata))
                formatstr = '\n'.join(loglines) + '\n\n'
                self.dl.handlers.append(LogHandlerFile(
                    fn, formatstr, prolog=prolog, missing='   ~   ',
                    skiprec=lambda d: 'meas_pulse_point' not in d))

        # amplitudes of sines vs freq
        if 'freq' in d['dataloggers']:
            freqs, ch2s = (d['dataloggers']['freq'][key]
                           for key in ('freqs', 'ch2s'))
            item = {'functype': 'F', 'typ': 'fampli'}
            for uubnum in self.uubnums:
                fn = self.datadir + ('fampli_uub%04d' % uubnum) +\
                     dt.strftime('-%Y%m%d.log')
                prolog = """\
# Amplitudes of sines depending on frequency
# UUB #%04d, date %s
# columns: timestamp | meas_freq_point | ch2 | freq [MHz] | """ % (
                    uubnum, dt.strftime('%Y-%m-%d'))
                prolog += ' | '.join(['fampli.ch%d' % chan
                                      for chan in self.chans])
                prolog += '\n'
                loglines = []
                for ch2 in ch2s:
                    for freq in freqs:
                        logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                                   '{meas_freq_point:2d}',
                                   '%d %5.2f' % (ch2, freq/1e6)]
                        logdata += [' '*7 if (chan in self.highgains and ch2)
                                    else '{%s:7.2f}' % item2label(
                                            item, uubnum=uubnum, chan=chan,
                                            freq=freq, ch2=ch2)
                                    for chan in self.chans]
                        loglines.append(' '.join(logdata))
                formatstr = '\n'.join(loglines) + '\n\n'
                self.dl.handlers.append(LogHandlerFile(
                    fn, formatstr, prolog=prolog, missing='   ~   ',
                    skiprec=lambda d: 'meas_freq_point' not in d))

        # linearity
        if d['dataloggers'].get('linearity', False):
            for uubnum in self.uubnums:
                fn = self.datadir + ('linear_uub%04d' % uubnum) +\
                     dt.strftime('-%Y%m%d.log')
                prolog = """\
# Linearity ADC count vs. voltage analysis
# - sensitivity [ADC count/mV] & correlation coefficient
# UUB #%04d, date %s
# columns: timestamp | meas_pulse_point""" % (uubnum, dt.strftime('%Y-%m-%d'))
                logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                           '{meas_pulse_point:2d}']
                for typ, fmt in (('sens', '6.3f'), ('corr', '7.5f')):
                    prolog += ''.join([' | %s.ch%d' % (typ, chan)
                                       for chan in self.chans])
                    logdata += ['{%s:%s}' % (item2label(
                        uubnum=uubnum, chan=chan, typ=typ), fmt)
                                for chan in self.chans]
                prolog += '\n'
                formatstr = ' '.join(logdata) + '\n'
                lh = LogHandlerFile(
                    fn, formatstr, prolog=prolog,
                    skiprec=lambda d: 'meas_pulse_point' not in d)
                lh.filters.append(dpfilter_linear)
                self.dl.handlers.append(lh)

        # fsensitivity TBD
        if d['dataloggers'].get('fsensitivity', False):
            pass

        # database
        if d['dataloggers'].get('db_pulse', False):
            itemr = {key: d['dataloggers']['pede'][key]
                     for key in ('voltage', 'ch2')}
            itemr['functype'] = 'P'
            prolog = """\
# Export to database
# date """ + dt.strftime('%Y%m%d') + "\n"
            fn = self.datadir + dt.strftime('db-%Y%m%d.js')
            logdata = []
            for uubnum in self.uubnums:
                for chan in self.chans:
                    items = [('meas_pulse_point', '{meas_pulse_point:d}'),
                             ('temp', '{set_temp:6.1f}'),
                             ('uub', '%d' % uubnum),
                             ('chan', '%d' % chan)]
                    for typ, fmt in (('pede', '7.2f'), ('pedesig', '7.2f')):
                        items.append((typ, '{%s:%s}' % (item2label(
                            itemr, uubnum=uubnum, chan=chan, typ=typ), fmt)))
                    for typ, fmt in (('sens', '6.3f'), ('corr', '7.5f')):
                        items.append((typ, '{%s:%s}' % (item2label(
                            uubnum=uubnum, chan=chan, typ=typ), fmt)))
                    linedata = '{{ ' + ', '.join(['"%s": %s' % item
                                                  for item in items]) + ' }}\n'
                    logdata.append(linedata)
            formatstr = ''.join(logdata)
            lh = LogHandlerFile(
                fn, formatstr, prolog=prolog, missing='"NaN"',
                skiprec=lambda d: 'db_pulse' not in d)
            lh.filters.append(dpfilter_linear)
            self.dl.handlers.append(lh)

        # pickle
        if d['dataloggers'].get('pickle', False):
            fn = self.datadir + dt.strftime('pickle-%Y%m%d')
            lh = LogHandlerPickle(fn)
            self.dl.handlers.append(lh)

        self.dl.start()

    def stop(self):
        """Stop all threads"""
        self.timer.stop.set()
        self.timer.evt.set()
        self.dl.stop.set()
        self.dp0.stop.set()
        self.ulisten.stop.set()
        self.uconv.stop.set()


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
