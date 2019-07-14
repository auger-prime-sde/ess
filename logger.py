"""
 ESS procedure
 logging
"""

import logging
import string
import threading
import pickle
from datetime import datetime, timedelta
from queue import Empty

from dataproc import item2label, float2expo


class MyFormatter(string.Formatter):
    """Formatter with default values for missing keys"""
    def __init__(self, missing='~', missing_keys=None):
        self.missing, self.missing_keys = missing, missing_keys

    def get_field(self, field_name, args, kwargs):
        # Handle a key not found
        try:
            val = super(MyFormatter, self).get_field(field_name, args, kwargs)
        except (KeyError, AttributeError):
            if (self.missing_keys is not None and
                    field_name in self.missing_keys):
                missing = self.missing_keys[field_name]
            else:
                missing = self.missing
            val = (None, missing), field_name
        return val

    def format_field(self, value, spec):
        # handle an invalid format
        if isinstance(value, tuple) and value[0] is None:
            return value[1]
        try:
            return super(MyFormatter, self).format_field(value, spec)
        except ValueError:
            return self.missing


class LogHandlerFile(object):
    def __init__(self, filename, formatstr, prolog='', skiprec=None,
                 missing='~', missing_keys=None):
        self.f = open(filename, 'a')
        self.f.write(prolog)
        self.f.flush()
        self.formatstr = formatstr
        formatter = MyFormatter(missing)
        # extract keys from formatstr
        self.keys = [p[1] for p in formatter.parse(formatstr)
                     if p[1] is not None]
        # filter missing_keys by keys
        if missing_keys is None:
            formatter.missing_keys = None
        else:
            formatter.missing_keys = dict((k, missing_keys[k])
                                          for k in self.keys
                                          if k in missing_keys)
        self.formatter = formatter
        self.skiprec = skiprec

    def write_rec(self, d):
        """Write one record to log
d - dictionary key: value"""
        if self.skiprec is not None and self.skiprec(d):
            return
        record = self.formatter.format(self.formatstr, **d)
        self.f.write(record)
        self.f.flush()

    def __del__(self):
        self.f.close()


class LogHandlerRamp(object):
    prolog = """\
# Ramp test results
# date %s
# tested UUBs: %s
# columns: timestamp | meas_point | OK
#   _or_   timestamp | meas_point | failed: <label list>
#   _or_   timestamp | meas_point | missing: <label list>
"""

    def __init__(self, filename, dt, uubnums):
        self.f = open(filename, 'a')
        uubs = ', '.join(['%04d' % uubnum for uubnum in uubnums])
        self.f.write(LogHandlerRamp.prolog % (dt.strftime('%Y-%m-%d'), uubs))
        self.f.flush()
        self.skiprec = lambda d: 'meas_ramp' not in d
        self.formatter = MyFormatter('~')
        self.formatstr = '{timestamp:%Y-%m-%dT%H:%M:%S} {meas_point:2d} '

    def write_rec(self, d):
        if self.skiprec is not None and self.skiprec(d):
            return
        recprefix = self.formatter.format(self.formatstr, **d)
        missing = d.get('ramp_missing', None)
        failed = d.get('ramp_failed', None)
        if failed:
            self.f.write(recprefix + 'failed: ' + ' '.join(failed) + '\n')
        if missing:
            self.f.write(recprefix + 'missing: ' + ' '.join(missing) + '\n')
        if failed is None and missing is None:
            self.f.write(recprefix + 'OK\n')
        self.f.flush()

    def __del__(self):
        self.f.close()


class LogHandlerPickle(object):
    """LogHandler saving all records as pickles to file."""
    def __init__(self, filename=None):
        if filename is None:
            filename = datetime.now().strftime('data/loghandler-%Y%m%d%H%M')
        self.fp = open(filename, 'a')
        logging.getLogger('LogHandlerPickle').info('saving to pickle file %s',
                                                   filename)

    def write_rec(self, d):
        pickle.dump(d, self.fp)

    def __del__(self):
        self.fp.close()


class DataLogger(threading.Thread):
    """Thread to save all results"""
    def __init__(self, q_resp, timeout=10):
        """Constructor.
q_resp - a queue with results to save
timeout - interval for collecting data
"""
        super(DataLogger, self).__init__()
        self.q_resp = q_resp
        self.timeout = timeout
        self.handlers = []
        self.filters = {None: None}
        self.records = {}
        self.stop = threading.Event()

    def add_handler(self, handler, filterlist=None):
        """Add handler and filters to apply for it"""
        if filterlist is None:
            key = None
        else:
            key = tuple([id(filt) for filt in filterlist])
        if key not in self.filters:
            self.filters[key] = filterlist
        self.handlers.append((handler, key))

    def run(self):
        logger = logging.getLogger('logger')
        last_ts = datetime(2016, 1, 1)  # minus infinity
        while not self.stop.is_set():
            if self.records:
                qtend = min([rec['tend'] for rec in self.records.values()])
            else:
                qtend = datetime.now() + timedelta(seconds=self.timeout)
            # logger.debug('tend = %s' %
            # datetime.strftime(tend, "%Y-%m-%d %H:%M:%S"))
            # read from queue until some record is timeouted
            while not self.stop.is_set():
                timeout = (qtend - datetime.now()).total_seconds()
                # logger.debug('timeout = %.6f' % timeout)
                if timeout < 0.0:
                    break
                try:
                    newrec = self.q_resp.get(True, timeout)
                except Empty:
                    # logger.debug('q_resp.get() timeout')
                    continue
                try:
                    ts = newrec.pop('timestamp')
                except AttributeError:
                    logger.debug('Wrong record: %s', repr(newrec))
                    continue

                if 'log_timeout' in newrec:
                    tout = max(int(newrec.pop('log_timeout')),
                               self.timeout)
                else:
                    tout = self.timeout
                recalc = tout > self.timeout
                tend = ts + timedelta(seconds=tout)
                if ts in self.records:
                    if tend > self.records[ts]['tend']:
                        newrec['tend'] = tend
                    else:
                        recalc = False
                    self.records[ts].update(newrec)
                elif ts > last_ts:  # add only ts after the last written
                    tend_curr = max(   # latest tend of previous recs
                        [rec['tend']
                         for ts1, rec in self.records.items()
                         if ts1 < ts] + [ts])
                    if tend <= tend_curr:
                        newrec['tend'] = tend_curr
                        recalc = False
                    else:
                        newrec['tend'] = tend
                    logger.debug(
                        'Added new record %s',
                        datetime.strftime(ts, "%Y-%m-%d %H:%M:%S"))
                    self.records[ts] = newrec
                else:
                    logger.info('Discarding an old record %s',
                                datetime.strftime(ts, "%Y-%m-%d %H:%M:%S"))
                    continue
                # eventually increase tend for newer records
                if recalc:
                    for ts1, rec in self.records.items():
                        if ts < ts1 and rec['tend'] < tend:
                            rec['tend'] = tend

            # process expired records
            tnow = datetime.now()
            expts = [ts for ts, rec in self.records.items()
                     if rec['tend'] <= tnow]
            for ts in sorted(expts):
                logger.debug('write rec for ts = %s',
                             datetime.strftime(ts, "%Y-%m-%d %H:%M:%S"))
                if ts > last_ts:
                    last_ts = ts
                rec = self.records.pop(ts)
                rec.pop('tend')
                rec['timestamp'] = ts
                # apply filters to rec
                recs = {}
                for key, filterlist in self.filters.items():
                    if key is None:
                        recs[None] = rec
                        continue
                    nrec = rec
                    for filt in filterlist:
                        nrec = filt(nrec)
                    recs[key] = nrec
                logger.debug('Rec written to handlers: %s', repr(recs))
                for h, key in self.handlers:
                    h.write_rec(recs[key])
        logger.info('run() finished, deleting handlers')
        for h, key in self.handlers:
            h.__del__()
        self.handlers = None

    def join(self, timeout=None):
        logging.getLogger('logger').debug('DataLogger.join')
        self.stop.set()   # stop run()
        super(DataLogger, self).join(timeout)


class QLogHandler(object):
    """A simple dispatcher of log records"""
    def handle(self, record):
        logger = logging.getLogger(record.name)
        logger.handle(record)


class QueDispatch(threading.Thread):
    """A simple dispatcher between queues with None as a sentinel"""
    def __init__(self, q_in, q_out, zLog=False, logname='QueDispatch'):
        self.q_in, self.q_out = q_in, q_out
        self.zLog = zLog
        self.logger = logging.getLogger(logname)

    def run(self):
        self.logger.info('Starting QueDispatch')
        while True:
            item = self.q_in.get()
            if item is None:
                break
            if self.zLog:
                self.logger.debug(repr(item))
            self.q_out.put(item)
        self.logger.info('QueDispatch Finished')


class QueView(threading.Thread):
    """Queue viewer
Consume items from queue and display them"""
    def __init__(self, timer, q):
        self.timer, self.q = timer, q
        self.timeout = 0.5
        super(QueView, self).__init__()

    def run(self):
        logger = logging.getLogger('QueView')
        while True:
            if self.timer.stop.is_set():
                logger.info('Timer stopped, stopping QueView')
                return
            try:
                item = self.q.get(True, self.timeout)
            except Empty:
                continue
            logger.debug(repr(item))


class QuePipeView(threading.Thread):
    """Queue viewer
Consume items from queue in, put them to queue out and display them"""
    def __init__(self, timer, q_in, q_out):
        self.timer, self.q_in, self.q_out = timer, q_in, q_out
        self.timeout = 0.5
        super(QuePipeView, self).__init__()

    def run(self):
        logger = logging.getLogger('QuePipeView')
        while True:
            if self.timer.stop.is_set():
                logger.info('Timer stopped, stopping QueView')
                return
            try:
                item = self.q_in.get(True, self.timeout)
                self.q_out.put(item)
            except Empty:
                continue
            logger.debug(repr(item))


# predefined LogHandlers
def makeDLtemperature(ctx, uubnums, sc=False):
    """Create LogHandlerFile for temperatures
ctx - context object, used keys: datadir + basetime
uubnums - list of UUB numbers to log
sc - if True, log also temperatures from SlowControl"""
    prolog = """\
# Temperature measurement: BME + chamber + Zynq
# date %s
# columns: timestamp | set.temp | BME1.temp | BME2.temp | chamber.temp""" % (
        ctx.basetime.strftime('%Y-%m-%d'))
    prolog += ''.join([' | UUB-%04d.zynq_temp' % uubnum
                       for uubnum in uubnums])
    if sc:
        prolog += ''.join(
            [' | UUB-%04d.sc_temp' % uubnum for uubnum in uubnums])
    prolog += '\n'
    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
               '{set_temp:6.1f}',
               '{bme_temp1:7.2f}',
               '{bme_temp2:7.2f}',
               '{chamber_temp:7.2f}']
    logdata += ['{zynq%04d_temp:5.1f}' % uubnum for uubnum in uubnums]
    if sc:
        logdata += ['{sc%04d_temp:5.1f}' % uubnum for uubnum in uubnums]
    formatstr = ' '.join(logdata) + '\n'
    fn = ctx.datadir + ctx.basetime.strftime('thp-%Y%m%d.log')
    return LogHandlerFile(fn, formatstr, prolog=prolog)


def makeDLslowcontrol(ctx, uubnum):
    """Create LogHandlerFile for SlowControl values
ctx - context object, used keys: datadir + basetime
uubnum - UUB number to log"""
    labels_I = ('1V', '1V2', '1V8', '3V3', '3V3_sc', 'P3V3', 'N3V3',
                '5V', 'radio', 'PMTs')
    labels_U = ('1V', '1V2', '1V8', '3V3', 'P3V3', 'N3V3',
                '5V', 'radio', 'PMTs', 'ext1', 'ext2')
    fn = ctx.datadir + ('sc_uub%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    prolog = """\
# Slow Control measured values
# UUB #%04d, date %s
# voltages in mV, currents in mA
# columns: timestamp""" % (uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}']
    prolog += ''.join([' | I_%s' % label for label in labels_I])
    logdata.extend(['{sc%04d_i_%s:5.2f}' % (uubnum, label)
                    for label in labels_I])
    prolog += ''.join([' | U_%s' % label for label in labels_U])
    logdata.extend(['{sc%04d_u_%s:7.2f}' % (uubnum, label)
                    for label in labels_U])
    prolog += '\n'
    formatstr = ' '.join(logdata) + '\n'
    return LogHandlerFile(fn, formatstr, prolog=prolog)


def makeDLpedestals(ctx, uubnum, count=None):
    """Create LogHandlerFile for pedestals and their sigma
ctx - context object, used keys: datadir + basetime + chans
uubnum - UUB to log"""
    fn = ctx.datadir + ('pede_uub%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    prolog = """\
# Pedestals and their std. dev.
# UUB #%04d, date %s
# columns: timestamp | meas_point""" % (
        uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    if count is not None:
        prolog += " | index"
    for typ, fmt in (('pede', '7.2f'), ('pedesig', '7.2f')):
        prolog += ''.join([' | %s.ch%d' % (typ, chan)
                           for chan in ctx.chans])
    prolog += '\n'

    if count is None:
        logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                   '{meas_point:3d}']
        for typ, fmt in (('pede', '7.2f'), ('pedesig', '7.2f')):
            logdata += ['{%s:%s}' % (item2label(
                functype='N', uubnum=uubnum, chan=chan, typ=typ), fmt)
                        for chan in ctx.chans]
        formatstr = ' '.join(logdata) + '\n'
    else:
        loglines = []
        for ind in range(count):
            logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                       '{meas_point:3d}', "%03d" % ind]
            for typ, fmt in (('pede', '7.2f'), ('pedesig', '7.2f')):
                logdata += ['{%s:%s}' % (
                    item2label(functype='N', uubnum=uubnum, chan=chan,
                               typ=typ, index=ind), fmt)
                            for chan in ctx.chans]
            loglines.append(' '.join(logdata) + '\n')
        formatstr = ''.join(loglines)
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: 'meas_noise' not in d)


def makeDLpedestalstat(ctx, uubnum):
    """Create LogHandlerFile for staticstics of pedestals (mean + std)
ctx - context object, used keys: datadir + basetime + chans
uubnum - UUB to log"""
    fn = ctx.datadir + ('pedestat_uub%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    prolog = """\
# Pedestals and their std. dev. - staticstics
# UUB #%04d, date %s
# columns: timestamp | meas_point""" % (
        uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
               '{meas_point:3d}']
    for typ, fmt in (('pedemean', '7.2f'), ('pedestddev', '7.2f'),
                     ('pedesigmean', '7.2f')):
        prolog += ''.join([' | %s.ch%d' % (typ, chan) for chan in ctx.chans])
        logdata += ['{%s:%s}' % (item2label(
            functype='N', uubnum=uubnum, chan=chan, typ=typ), fmt)
                    for chan in ctx.chans]
    prolog += '\n'
    formatstr = ' '.join(logdata) + '\n'
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: 'meas_noise' not in d)


def makeDLhsampli(ctx, uubnum, keys):
    """Create LogHandlerFile for halfsine amplitudes
ctx - context object, used keys: datadir + basetime + highgains + chans
      afg.params, splitmode
uubnum - UUB to log
keys - voltages and/or splitmodes and/or count"""
    if keys is None:
        keys = {}
    voltages = keys.get('voltages', (ctx.afg.param['Pvoltage'], ))
    splitmodes = keys.get('splitmodes', (ctx.splitmode(), ))
    if 'count' in keys:
        indices = range(keys['count'])
    else:
        indices = (None, )
    fn = ctx.datadir + ('ampli_uub%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    prolog = """\
# Amplitudes of halfsines
# UUB #%04d, date %s
# columns: timestamp | meas_point | splitmode | voltage | """ % (
        uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    itemr = {'functype': 'P', 'typ': 'ampli', 'uubnum': uubnum}
    if indices[0] is not None:
        prolog += "index | "
    prolog += ' | '.join(['ampli.ch%d' % chan for chan in ctx.chans])
    prolog += '\n'
    loglines = []
    for splitmode in splitmodes:
        for voltage in voltages:
            for ind in indices:
                logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                           '{meas_point:2d}',
                           '%d %.2f' % (splitmode, voltage)]
                if ind is not None:
                    logdata.append('%03d' % ind)
                    itemr['index'] = ind
                logdata += [' '*7 if (chan in ctx.highgains and splitmode > 0)
                            else '{%s:7.2f}' % item2label(
                                    itemr, chan=chan,
                                    voltage=voltage, splitmode=splitmode)
                            for chan in ctx.chans]
                loglines.append(' '.join(logdata) + '\n')
    formatstr = ''.join(loglines)
    return LogHandlerFile(fn, formatstr, prolog=prolog, missing='   ~   ',
                          skiprec=lambda d: 'meas_pulse' not in d)


def makeDLfampli(ctx, uubnum, keys):
    """Create LogHandlerFile for sine amplitudes
ctx - context object, used keys: datadir + basetime + highgains + chans
uubnum - UUB to log
keys - freqs, voltages and/or splitmodes"""
    if keys is None:
        keys = {}
    voltages = keys.get('voltages', (ctx.afg.param['Fvoltage'], ))
    splitmodes = keys.get('splitmodes', (ctx.splitmode(), ))
    freqs = keys.get('freqs', (ctx.afg.param['freq'], ))
    if 'count' in keys:
        indices = range(keys['count'])
    else:
        indices = (None, )
    fn = ctx.datadir + ('fampli_uub%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    prolog = """\
# Amplitudes of sines depending on frequency
# UUB #%04d, date %s
# columns: timestamp | meas_point | flabel | freq [MHz] \
| splitmode | voltage | """ % (uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    itemr = {'functype': 'F', 'typ': 'fampli', 'uubnum': uubnum}
    if indices[0] is not None:
        prolog += "index | "
    prolog += ' | '.join(['fampli.ch%d' % chan for chan in ctx.chans])
    prolog += '\n'
    loglines = []
    for freq in freqs:
        flabel = float2expo(freq, manlength=3)
        for splitmode in splitmodes:
            for voltage in voltages:
                for ind in indices:
                    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                               '{meas_point:2d}',
                               '%3s %5.2f' % (flabel, freq/1e6),
                               '%d %.2f' % (splitmode, voltage)]
                    if ind is not None:
                        logdata.append('%03d' % ind)
                        itemr['index'] = ind
                    logdata += [' '*7 if (chan in ctx.highgains and
                                          splitmode > 0)
                                else '{%s:7.2f}' % item2label(
                                        itemr, chan=chan, flabel=flabel,
                                        voltage=voltage, splitmode=splitmode)
                                for chan in ctx.chans]
                    loglines.append(' '.join(logdata) + '\n')
    formatstr = ''.join(loglines)
    return LogHandlerFile(fn, formatstr, prolog=prolog, missing='   ~   ',
                          skiprec=lambda d: 'meas_freq' not in d)


def makeDLlinear(ctx, uubnum):
    """Create LogHandlerFile for gain and corr. coeff
ctx - context object, used keys: datadir + basetime + chans
uubnum - UUB to log"""
    fn = ctx.datadir + ('linear_uub%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    prolog = """\
# Linearity ADC count vs. voltage analysis
# - gain [ADC count/mV] & correlation coefficient
# UUB #%04d, date %s
# columns: timestamp | meas_point""" % (
        uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
               '{meas_point:2d}']
    itemr = {'functype': 'P', 'uubnum': uubnum}
    for typ, fmt in (('gain', '6.3f'), ('lin', '7.5f')):
        prolog += ''.join([' | %s.ch%d' % (typ, chan)
                           for chan in ctx.chans])
        logdata += ['{%s:%s}' % (item2label(itemr, chan=chan, typ=typ), fmt)
                    for chan in ctx.chans]
    prolog += '\n'
    formatstr = ' '.join(logdata) + '\n'
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: 'meas_pulse' not in d)


def makeDLfreqgain(ctx, uubnum, freqs):
    """Create LogHandlerFile for gain and corr. coeff
ctx - context object, used keys: datadir + basetime + chans
uubnum - UUB to log
freqs - list of frequencies to log"""
    fn = ctx.datadir + ('fgain_uub%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    prolog = """\
# Frequency dependent gain ADC count vs. voltage analysis
# - freqgain [ADC count/mV] & correlation coefficient
# UUB #%04d, date %s
# columns: timestamp | meas_point | flabel | freq [MHz]""" % (
        uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    prolog += ''.join([' | fgain.ch%d | flin.ch%d' % (chan, chan)
                       for chan in ctx.chans])
    prolog += '\n'
    itemr = {'functype': 'F', 'uubnum': uubnum}
    loglines = []
    for freq in freqs:
        flabel = float2expo(freq, manlength=3)
        logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                   '{meas_point:2d}',
                   '%3s %5.2f' % (flabel, freq/1e6)]
        for typ, fmt in (('fgain', '6.3f'), ('flin', '7.5f')):
            logdata += ['{%s:%s}' % (item2label(
                itemr, flabel=flabel, chan=chan, typ=typ), fmt)
                        for chan in ctx.chans]
        loglines.append(' '.join(logdata) + '\n')
    formatstr = ''.join(loglines)
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: 'meas_freq' not in d)


def makeDLcutoff(ctx, uubnum):
    """Create LogHandlerFile for frequency cut-off
ctx - context object, used keys: datadir + basetime + chans
uubnum - UUB to log"""
    fn = ctx.datadir + ('cutoff_uub%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    prolog = """\
# Cut-off frequency [MHz]
# UUB #%04d, date %s
# columns: timestamp | meas_point""" % (
        uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    prolog += ''.join([' | cutoff.ch%d' % chan for chan in ctx.chans]) + '\n'
    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
               '{meas_point:2d}']
    itemr = {'uubnum': uubnum, 'typ': 'cutoff'}
    logdata += ['{%s:5.2f}' % item2label(itemr, chan=chan)
                for chan in ctx.chans]
    formatstr = ' '.join(logdata) + '\n'
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: 'meas_freq' not in d)
