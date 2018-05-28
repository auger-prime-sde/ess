"""
 ESS procedure
 main program
"""

import os
import json
import logging
from datetime import datetime, timedelta
from Queue import Queue

# ESS stuff
from timer import Timer, periodic_ticker, one_tick
from logger import LogHandlerFile, DataLogger, skiprec_MP
from BME import BME
from UUB import UUBtsc, UUBdisp, UUBmeas, gener_voltage_ch2
from chamber import Chamber, ChamberTicker
from dataproc import DataProcessor, item2label, DP_pede, DP_hsampli
from dataproc import DP_store, dpfilter_linear
from afg import AFG

VERSION = '20180524'

class ESS(object):
    """ESS process implementation"""

    def __init__(self, js):
        if hasattr(js, 'read'):
            d = json.load(js)
        else:
            d = json.loads(js)
        if 'logging' in d:
            kwargs = {key: d['logging'][key] for key in ('level', 'format')
                          if key in d['logging']}
            logging.basicConfig(**kwargs)
        logger = logging.getLogger('ess')

        dt = datetime.now()
        dt = dt.replace(second=0, microsecond=0, minute=dt.minute+1)
        self.timer = Timer(dt)
        self.timer.start()

        # queues
        self.q_resp = Queue()
        self.q_dp = Queue()

        # datadir
        self.datadir = dt.strftime('data-%Y%m%d/')
        if not os.path.isdir(self.datadir):
            os.mkdir(self.datadir)
        
        # tickers
        thp_period = d['tickers'].get('meas.thp', 30)
        self.timer.add_ticker('meas.thp', periodic_ticker(thp_period))
        if 'chamberticker' in d['tickers']:
            fn = d['tickers']['chamberticker']
            with open(fn, 'r') as fp:
                self.chticker = ChamberTicker(fp, self.timer, self.q_resp)
            self.chticker.start()

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
        # default_voltage = (0.8, 1.0, 1.2, 1.4, 1.6)
        # default_ch2=('off', 'on')
        default_voltage = (1.8, )
        default_ch2=('off', )
        self.afg = AFG(timer=1e-3)
        # width of halfsine in microseconds
        self.hswidth = 1.0e6 / self.afg.param['freq'] / 20 
        gener = gener_voltage_ch2(default_voltage=default_voltage,
                                  default_ch2=default_ch2)

       # UUBs - UUBdisp and UUBmeas
        self.uubnums = [int(uubnum) for uubnum in d['uubnums']]
        self.udisp = UUBdisp(self.timer, self.afg, gener)
        self.uubmeas = {uubnum: UUBmeas(uubnum, self.udisp, self.q_dp)
                     for uubnum in self.uubnums}
        self.udisp.start()
        for uub in self.uubmeas.itervalues():
            uub.start()

        # UUBs - Zync temperature & SlowControl
        self.uubtsc = {uubnum: UUBtsc(uubnum, self.timer, self.q_resp)
                     for uubnum in self.uubnums}
        for uub in self.uubtsc.itervalues():
            uub.start()

        # data processing
        self.dp0 = DataProcessor(self.q_dp)
        self.dp0.workhorses.append(DP_pede(self.q_resp))
        self.dp0.workhorses.append(DP_hsampli(self.q_resp, self.hswidth))
        self.dp0.workhorses.append(DP_store(self.datadir))
        self.dp0.start()

        # DataLogger & handlers
        # handler for amplitudes
        lowgains = (1, 3, 5, 7, 9)
        highgains = (2, 4, 6, 10)
        chans = sorted(lowgains+highgains)

        self.dl = DataLogger(self.q_resp)
        # temperature
        if d['dataloggers'].get('temperature', False):
            prolog = """# Temperature measurement: BME + chamber + Zynq
# date %s
# columns: timestamp | set.temp | BME1.temp | BME2.temp | chamber.temp | """ % (
                  dt.strftime('%Y-%m-%d'))
            prolog += ' | '.join([ 'UUB-%04d.zynq_temp' % uubnum
                                   for uubnum in self.uubnums])
            prolog += '\n'
            logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                       '{set_temp:6.1f}',
                       '{bme_temp1:7.2f}',
                       '{bme_temp2:7.2f}',
                       '{chamber_temp:7.2f}']
            logdata += ['{zynq%04d_temp:5.1f}' % uubnum
                        for uubnum in self.uubnums]
            formatstr = ' '.join(logdata) + '\n'
            fn = self.datadir + dt.strftime('thp-%Y%m%d.log')
            self.dl.handlers.append(LogHandlerFile(fn, formatstr,
                                                   prolog=prolog))
        
        # pedestals & their std
        if 'pede' in d['dataloggers']:
            voltage, ch2 = (d['dataloggers']['pede'][key]
                            for key in ('voltage', 'ch2'))
            for uubnum in self.uubnums:
                fn = self.datadir + ('pede_uub%04d' % uubnum) +\
                     dt.strftime('-%Y%m%d.log')
                prolog = """# Pedestals and their std. dev.
# UUB #%04d, date %s
# columns: timestamp | meas_point""" % ( uubnum, dt.strftime('%Y-%m-%d'))
                logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}', '{meas_point:2d}']
                for typ, fmt in (('pede', '4.0f'), ('pedesig', '7.2f')):
                    prolog += ''.join([' | %s.ch%d' % (typ, chan)
                                       for chan in chans])
                    logdata += ['{%s:%s}' % (item2label(
                        {'uubnum': uubnum, 'voltage': voltage, 'ch2': ch2},
                        chan=chan, typ=typ), fmt)
                                for chan in chans]
                prolog += '\n'
                formatstr = ' '.join(logdata) + '\n'
                lh = LogHandlerFile(fn, formatstr, prolog=prolog,
                                    skiprec=skiprec_MP)
                self.dl.handlers.append(lh)
        
        # amplitudes of halfsines
        if 'ampli' in d['dataloggers']:
            voltages, ch2s = (d['dataloggers']['ampli'][key]
                              for key in ('voltages', 'ch2s'))
            for uubnum in self.uubnums:
                fn = self.datadir + ('ampli_uub%04d' % uubnum) +\
                     dt.strftime('-%Y%m%d.log')
                prolog = """# Amplitudes of halfsines
# UUB #%04d, date %s
# columns: timestamp | meas_point | ch2 | voltage | """ % (
                     uubnum, dt.strftime('%Y-%m-%d'))
                prolog += ' | '.join(['ampli.ch%d' % chan for chan in chans])
                prolog += '\n'
                loglines = []
                for ch2 in ch2s:
                    for voltage in voltages:
                        logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                                   '{meas_point:2d}',
                                   '%d %.1f' % (ch2, voltage)]
                        logdata += [' '*7 if chan in highgains and ch2 == 'on'
                                    else '{%s:7.2f}' % item2label(
                                            {'uubnum': uubnum,
                                             'voltage': voltage,
                                             'ch2': ch2},
                                            typ='ampli', chan=chan)
                                    for chan in chans]
                        loglines.append(' '.join(logdata))
                formatstr = '\n'.join(loglines) + '\n\n'
                self.dl.handlers.append(LogHandlerFile(fn, formatstr,
                                                       prolog=prolog,
                                                       skiprec=skiprec_MP))

        # linearity
        if d['dataloggers'].get('linearity', False):
            for uubnum in self.uubnums:
                fn = self.datadir + ('linear_uub%04d' % uubnum) +\
                     dt.strftime('-%Y%m%d.log')
                prolog = """# Linearity ADC count vs. voltage analysis
# - sensitivity [ADC count/mV] & correlation coefficient
# UUB #%04d, date %s
# columns: timestamp | meas_point""" % (uubnum, dt.strftime('%Y-%m-%d'))
                logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}', '{meas_point:2d}']
                for typ, fmt in (('sens', '6.3f'), ('corr', '7.5f')):
                    prolog += ''.join([' | %s.ch%d' % (typ, chan)
                                       for chan in chans])
                    logdata += ['{%s:%s}' % (
                        item2label({'uubnum': uubnum}, chan=chan, typ=typ), fmt)
                                for chan in chans]
                prolog += '\n'
                formatstr = ' '.join(logdata) + '\n'
                lh = LogHandlerFile(fn, formatstr, prolog=prolog, skiprec=skiprec_MP)
                lh.filters.append(dpfilter_linear)
                self.dl.handlers.append(lh)

        self.dl.start()