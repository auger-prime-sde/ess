"""
 ESS procedure
 logging
"""

import os.path
import logging
import string
import threading
import traceback
import pickle
import json
import ssl
from datetime import datetime, timedelta
from http.client import HTTPSConnection
from queue import Empty

from dataproc import item2label, float2expo
from threadid import syscall, SYS_gettid
NOTCALC = '9999.99'  # number not calculated as overflow expected


class ExceptionLogger(object):
    """Log exception details into separate file"""
    fn_template = '{datadir:s}/exception_{count:04d}'

    def __init__(self, datadir):
        self.datadir = datadir
        self.count = 0
        self._lock = threading.Lock()

    def log(self, **kwargs):
        ts = datetime.now()
        with self._lock:
            count = self.count
            self.count = count + 1
        fn = self.fn_template.format(self.datadir, count)
        with open(fn, 'w') as fp:
            fp.write('ts = %s\n--- exception ---\n' % repr(ts))
            traceback.print_exc(file=fp)
            fp.write('--- /exception ---\n')
            for key, val in kwargs.items():
                fp.write('%s = %s\n' % (key, repr(val)))


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
        self.f = None
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
        self.label = 'LogHandlerFile:' + os.path.basename(filename)

    def write_rec(self, d):
        """Write one record to log
d - dictionary key: value"""
        if self.skiprec is not None and self.skiprec(d):
            return
        record = self.formatter.format(self.formatstr, **d)
        self.f.write(record)
        self.f.flush()

    def stop(self):
        if self.f is not None:
            self.f.close()
            self.f = None

    def __del__(self):
        self.stop()


class LogHandlerRamp(object):
    prolog = """\
# Ramp test results
# date %s
# tested UUBs: %s
# columns: timestamp | meas_point | set_temp | OK
#   _or_   timestamp | meas_point | set_temp | failed: <label list>
#   _or_   timestamp | meas_point | set_temp | missing: <label list>
"""

    def __init__(self, filename, dt, uubnums):
        self.f = None
        self.f = open(filename, 'a')
        uubs = ', '.join(['%04d' % uubnum for uubnum in uubnums])
        self.f.write(LogHandlerRamp.prolog % (dt.strftime('%Y-%m-%d'), uubs))
        self.f.flush()
        self.skiprec = lambda d: 'meas_ramp' not in d
        self.formatter = MyFormatter('~')
        self.formatstr = ('{timestamp:%Y-%m-%dT%H:%M:%S} {meas_point:4d} ' +
                          '{set_temp:5.1f} ')
        self.label = 'LogHandlerRamp'

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

    def stop(self):
        if self.f is not None:
            self.f.close()
            self.f = None

    def __del__(self):
        self.stop()


class LogHandlerPickle(object):
    """LogHandler saving all records as pickles to file."""
    def __init__(self, filename=None):
        self.fp = None
        if filename is None:
            filename = datetime.now().strftime('data/pickle-%Y%m%d%H%M')
        self.fp = open(filename, 'ab')
        logging.getLogger('LogHandlerPickle').info('saving to pickle file %s',
                                                   filename)
        self.label = 'LogHandlerPickle'

    def write_rec(self, d):
        pickle.dump(d, self.fp)

    def stop(self):
        if self.fp is not None:
            self.fp.close()
            self.fp = None

    def __del__(self):
        self.stop()


class LogHandlerGrafana(object):
    """LogHandler delivering data to Grafana database"""
    TEMP_KEYS = (('set_temp', 'temp_set'),
                 ('chamber_temp', 'temp_chamber'),
                 ('bme_temp1', 'temp_BME1'),
                 ('bme_temp2', 'temp_BME2'),
                 ('ds0_temp', 'temp_DS0'),
                 ('ds1_temp', 'temp_DS1'),
                 ('ds2_temp', 'temp_DS2'),
                 ('ds3_temp', 'temp_DS3'),
                 ('ds4_temp', 'temp_DS4'),
                 ('ds5_temp', 'temp_DS5'),
                 ('ds6_temp', 'temp_DS6'),
                 ('ds7_temp', 'temp_DS7'),
                 ('ds8_temp', 'temp_DS8'),
                 ('ds9_temp', 'temp_DS9'))
    SLOW_KEYS = (('zynq{u:04d}_temp', 'temp_zynq'),
                 ('sc{u:04d}_temp', 'temp_sc'),
                 ('itot_u{u:04d}', 'i_tot'),
                 ('sc{u:04d}_u_1V', 'u_1V'),
                 ('sc{u:04d}_i_1V', 'i_1V'),
                 ('sc{u:04d}_u_1V2', 'u_1V2'),
                 ('sc{u:04d}_i_1V2', 'i_1V2'),
                 ('sc{u:04d}_u_1V8', 'u_1V8'),
                 ('sc{u:04d}_i_1V8', 'i_1V8'),
                 ('sc{u:04d}_u_3V3', 'u_3V3'),
                 ('sc{u:04d}_i_3V3', 'i_3V3'),
                 ('sc{u:04d}_i_3V3_sc', 'i_3V3_sc'),
                 ('sc{u:04d}_u_P3V3', 'u_P3V3'),
                 ('sc{u:04d}_i_P3V3', 'i_P3V3'),
                 ('sc{u:04d}_u_N3V3', 'u_N3V3'),
                 ('sc{u:04d}_i_N3V3', 'i_N3V3'),
                 ('sc{u:04d}_u_5V', 'u_5V'),
                 ('sc{u:04d}_i_5V', 'i_5V'))
    KEYMAP = {'pedemean': 'pede',
              'pedestdev': 'pedesig',
              'noisemean': 'noise',
              'noisestdev': 'noisesig'}

    def __init__(self, starttime, uubnums, dbinfo):
        """Constructor.
starttime - program start
uubnums - list of UUBs in test
dbinfo - dict with configuration for Grafana
    host_addr - DNS name or IP address of a server to connect
    host_port - (HTTPS) port of the server to connect
    server_cert - server's certificate
    client_key - client's private key
    client_cert - client's certificate
    urlInit - URL to initialize TestRun
    urlSetStarttime - URL to set starttime
    urlWriteRec - URL to write record
    flabels - list of frequencies to log (as labels)
    fmtdatetime - datetime.strftime format for starttime"""
        self.logger = logging.getLogger('LHGrafana')
        self.uubnums = uubnums
        self.dbinfo = dbinfo
        self.flabels = self.dbinfo.pop('flabels', ())
        self.sslctx = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH, cafile=dbinfo['server_cert'])
        self.sslctx.load_cert_chain(certfile=dbinfo['client_cert'],
                                    keyfile=dbinfo['client_key'])
        param = '&'.join(['uub%d=%d' % (i+1, uubnum)
                          for i, uubnum in enumerate(uubnums)
                          if uubnum is not None])
        if starttime is not None:
            param = 'starttime=%s&%s' % (
                starttime.strftime(self.dbinfo['fmtdatetime']), param)
        self.logger.info('Initialize Grafana TestRun')
        conn = HTTPSConnection(self.dbinfo['host_addr'],
                               port=self.dbinfo['host_port'],
                               context=self.sslctx)
        # conn.set_debuglevel(3)
        conn.request("GET", self.dbinfo['urlInit'] + param)
        resp = conn.getresponse()
        self.logger.debug('Received status %d', resp.status)
        self.runid = int(resp.read())
        conn.close()
        self.logger.info('Received runid: %d', self.runid)
        self.label = 'LogHandlerGrafana'

    def setStarttime(self, starttime):
        self.logger.info('Setting starttime')
        url = self.dbinfo['urlSetStarttime'].format(
            runid=self.runid,
            starttime=starttime.strftime(self.dbinfo['fmtdatetime']))
        conn = HTTPSConnection(self.dbinfo['host_addr'],
                               port=self.dbinfo['host_port'],
                               context=self.sslctx)
        # conn.set_debuglevel(3)
        conn.request("GET", url)
        resp = conn.getresponse()
        self.logger.debug('Received status %d', resp.status)
        conn.close()

    def write_rec(self, d):
        common = {'timestamp': d['timestamp'].strftime(
            self.dbinfo['fmtdatetime'])}
        if 'rel_time' in d:
            common['rel_time'] = d['rel_time']
        res = {}
        # Temperature
        if 'meas_thp' in d:
            temper = {dbkey: d[esskey] for esskey, dbkey in self.TEMP_KEYS
                      if esskey in d}
            if temper:
                res['temperature'] = temper
        # SlowValues
        slowvals = []
        if 'meas_thp' in d or 'meas_sc' in d:
            for uubnum in self.uubnums:
                if uubnum is None:
                    continue
                vals = {}
                for keytemplate, dbname in self.SLOW_KEYS:
                    key = keytemplate.format(u=uubnum)
                    if key in d:
                        vals[dbname] = d[key]
                if vals:
                    vals['uubnum'] = uubnum
                    slowvals.append(vals)
        if slowvals:
            res['slowvals'] = slowvals
        # UUB data
        uubdata = []
        for uubnum in self.uubnums:
            if uubnum is None:
                continue
            for typ in ('pede', 'pedemean', 'pedestdev', 'cutoff',
                        'noise', 'noisemean', 'noisestdev', 'gain'):
                vals = self._collect(d, uubnum, typ)
                if vals is not None:
                    uubdata.append({'typ': self._keymap(typ),
                                    'uubnum': uubnum, 'values': vals})
            for flabel in self.flabels:
                vals = self._collect(d, uubnum, 'fgain', flabel)
                if vals is not None:
                    uubdata.append({'uubnum': uubnum, 'typ': 'freqgain',
                                    'flabel': flabel, 'values': vals})
        if uubdata:
            res['uubdata'] = uubdata
        # send results if any
        if res:
            res['common'] = common
            payload = json.dumps(res)
            self.logger.debug('Sending results to grafana')
            url = self.dbinfo['urlWriteRec'].format(
                runid=self.runid)
            conn = HTTPSConnection(self.dbinfo['host_addr'],
                                   port=self.dbinfo['host_port'],
                                   context=self.sslctx)
            # conn.set_debuglevel(3)
            conn.request("POST", url, body=bytes(payload, 'ascii'),
                         headers={'Content-Type': 'application/json'})
            resp = conn.getresponse()
            self.logger.debug('Received status %d', resp.status)
            conn.close()

    def _collect(self, d, uubnum, typ, flabel=None):
        """Collects data of type <typ> (+flabel) for UUB <uubnum>
return list of 10 values (or None) or None if no data available"""
        item = {'uubnum': uubnum, 'typ': typ}
        if typ == 'gain':
            item['functype'] = 'P'
        elif typ == 'fgain':
            item['functype'] = 'F'
            item['flabel'] = flabel
        elif typ in ('pede', 'pedemean', 'pedestdev',
                     'noise', 'noisemean', 'noisestdev'):
            item['functype'] = 'N'
        vals = {}
        for chan in range(1, 11):
            label = item2label(item, chan=chan)
            if label in d:
                vals[chan] = d[label]
        if vals:
            res = [None] * 10
            for chan, val in vals.items():
                res[chan-1] = val
            return res
        return None

    def _keymap(self, key):
        return LogHandlerGrafana.KEYMAP.get(key, key)

    def stop(self):
        pass


class LogHandlerVoltramp(object):
    """LogHandler for voltage ramp power on/off checks"""
    prolog = """\
# Power on/off test with voltage ramp up/down with expected state at end on/off
# date %s
# tested UUBs: %s
# columns: timestamp | meas_point | set_temp | <up/down> | <on/off> | """
    fmtprefix = '{timestamp:%Y-%m-%dT%H:%M:%S} {meas_point:4d} ' + \
                '{set_temp} {direction:4s} {state:3s}'

    def __init__(self, filename, dt, uubnums):
        self.fp = None
        self.fp = open(filename, 'a')
        self.uubnums = uubnums
        self.fp.write(LogHandlerVoltramp.prolog % (
            dt.strftime('%Y-%m-%d'),
            ', '.join(["%04d" % uubnum for uubnum in uubnums])))
        self.fp.write(' | '.join(['volt #%04d' % uubnum
                                  for uubnum in uubnums]))
        self.fp.write('\n')
        self.fp.flush()
        self.label = 'LogHandlerVoltramp'

    def write_rec(self, d):
        if 'volt_ramp' not in d:
            return
        direction, state = d['volt_ramp']
        set_temp = '%5.1f' % d['set_temp'] if 'set_temp' in d else '  ~  '
        prefix = LogHandlerVoltramp.fmtprefix.format(
            timestamp=d['timestamp'], meas_point=d['meas_point'],
            set_temp=set_temp, direction=direction, state=state)
        labeltemplate = 'voltramp' + direction + state + '_u%04d'
        values = []
        for uubnum in self.uubnums:
            label = labeltemplate % uubnum
            if label in d:
                values.append(' %4.1f' % d[label])
            else:
                values.append('  ~  ')
        self.fp.write(prefix + ''.join(values) + '\n')

    def stop(self):
        if self.fp is not None:
            self.fp.close()
            self.fp = None

    def __del__(self):
        self.stop()


class DataLogger(threading.Thread):
    """Thread to save all results"""
    def __init__(self, q_resp, timeout=10, elogger=None):
        """Constructor.
q_resp - a queue with results to save
timeout - interval for collecting data
elogger - exception logger
"""
        super(DataLogger, self).__init__()
        self.q_resp = q_resp
        self.timeout = timeout
        self.elogger = elogger
        self.handlers = []  # (handler, key, uubnum)
        self.filters = []   # (key, start, filterlist)
        self.records = {}
        self.stop = threading.Event()
        self.uubnums2del = []

    def add_handler(self, handler, filterlist=None, uubnum=None):
        """Add handler and filters to apply for it
filterlist - [(filter, filterlabel), ...]
Ignore None filters
if uubnum provided, the handler is removed when uubnum is removed"""
        if filterlist is None:
            key = None
        else:
            filterlist = [(filt, filtlabel) for (filt, filtlabel) in filterlist
                          if filt is not None]
            key = tuple([id(filt) for (filt, filtlabel) in filterlist])
            if key == ():
                key = None
        self.handlers.append((handler, key, uubnum))
        currkeys = [rec[0] for rec in self.filters]
        if key is None or key in currkeys:
            return
        # insert new key into self.filter evenutally using parent/child chains
        i = 0
        n = len(currkeys)
        while True:
            if i == n or key < currkeys[i]:
                break
            i += 1
        # check if exists j < i so that currkeys[j] is a parent of key
        for pkey in currkeys[i-1::-1]:
            if key[:len(pkey)] == pkey:
                start = pkey
                filterlist = filterlist[len(pkey):]
                break
        else:
            start = None
        # modify ckey if key can be parent
        for j in range(i, n):
            ckey = currkeys[j]
            if key == ckey[:len(key)]:
                nfilt = len(ckey) - len(key)
                cfiltlist = self.filters[j][2][-nfilt:]
                self.filters[j] = (ckey, pkey, cfiltlist)
        self.filters.insert(i, (key, start, filterlist))

    def run(self):
        logger = logging.getLogger('logger')
        tid = syscall(SYS_gettid)
        logger.debug('run start, name %s, tid %d', self.name, tid)
        last_ts = datetime(2016, 1, 1)  # minus infinity
        logger.info('starting run()')
        while not self.stop.is_set() or self.records:
            while self.uubnums2del:
                uubnum = self.uubnums2del.pop()
                nhandlers = []
                for rec in self.handlers:
                    if rec[2] == uubnum:  # remove uub
                        rec[0].stop()     # stop the handler
                    else:
                        nhandlers.append(rec)
                self.handlers = nhandlers
                # remove unused filters
                nfilterkeys = set([rec[1] for rec in self.handlers])
                self.filters = [rec for rec in self.filters
                                if rec[0] in nfilterkeys]
            if self.records:
                qtend = min([rec['tend'] for rec in self.records.values()])
            else:
                qtend = datetime.now() + timedelta(seconds=self.timeout)
            # logger.debug('tend = %s' %
            # datetime.strftime(tend, "%Y-%m-%d %H:%M:%S"))
            # read from queue until some record is timeouted
            while True:
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
                recs = {None: rec}
                for key, start, filterlist in self.filters:
                    nrec = recs[start]
                    for filt, filtlabel in filterlist:
                        try:
                            nrec = filt(nrec)
                        except Exception:
                            if self.elogger is not None:
                                self.elogger.log(
                                    filtlabel=filtlabel, nrec=nrec)
                            logger.error('filter %s raised exception',
                                         filtlabel)
                    recs[key] = nrec
                # logger.debug('Rec written to handlers: %s', repr(recs))
                for h, key, uubnum in self.handlers:
                    try:
                        h.write_rec(recs[key])
                    except Exception:
                        if self.elogger is not None:
                            self.elogger.log(h_label=h.label, rec=recs[key])
                        logger.error('%s.write_rec raised exception', h.label)

        logger.info('run() finished, deleting handlers')
        for h, key, uubnum in self.handlers:
            h.stop()
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
        super(QueDispatch, self).__init__()
        self.q_in, self.q_out = q_in, q_out
        self.zLog = zLog
        self.logger = logging.getLogger(logname)

    def run(self):
        tid = syscall(SYS_gettid)
        self.logger.info('Starting QueDispatch, name %s, tid %d',
                         self.name, tid)
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
        tid = syscall(SYS_gettid)
        logger.debug('run start, name %s, tid %d', self.name, tid)
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
        tid = syscall(SYS_gettid)
        logger.debug('run start, name %s, tid %d', self.name, tid)
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
def makeDLtemperature(ctx, uubnums, sc=False, dslist=()):
    """Create LogHandlerFile for temperatures
ctx - context object, used keys: datadir + basetime
uubnums - list of UUB numbers to log
sc - if True, log also temperatures from SlowControl
dslist - list of DS18B20 labels to log (integers in label 'DS%d')"""
    prolog = """\
# Temperature measurement: BME + chamber + Zynq
# date %s
# columns: timestamp | set.temp | BME1.temp | BME2.temp | chamber.temp""" % (
        ctx.basetime.strftime('%Y-%m-%d'))
    prolog += ''.join([' | DS%d.temp' % i for i in dslist])
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
    logdata += ['{ds%d_temp:5.1f}' % i for i in dslist]
    logdata += ['{zynq%04d_temp:5.1f}' % uubnum for uubnum in uubnums]
    if sc:
        logdata += ['{sc%04d_temp:5.1f}' % uubnum for uubnum in uubnums]
    formatstr = ' '.join(logdata) + '\n'
    fn = ctx.datadir + ctx.basetime.strftime('thp-%Y%m%d.log')
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: 'meas_thp' not in d)


def makeDLhumid(ctx, uubnums, scuubs=False):
    """Create LogHandlerFile for temperatures
ctx - context object, used keys: datadir + basetime
uubnums - list of UUB numbers to log
scuubs - list of uubs to log sc humid
         if True, use uubnums"""
    if scuubs is True:
        scuubs = uubnums
    else:
        assert set(scuubs) in set(uubnums)
    prolog = """\
# Humidity measurement: BME + chamber + [SlowControl]
# date %s
# columns: timestamp | set.humid | BME1.humid | BME2.humid |\
 chamber.humid""" % ctx.basetime.strftime('%Y-%m-%d')
    prolog += ''.join([' | UUB-%04d.sc_humid' % uubnum
                       for uubnum in scuubs])
    prolog += '\n'
    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
               '{set_humid:5.1f}',
               '{bme_humid1:6.2f}',
               '{bme_humid2:6.2f}',
               '{chamber_humid:6.2f}']
    logdata += ['{sc%04d_humid:6.2f}' % uubnum for uubnum in scuubs]
    formatstr = ' '.join(logdata) + '\n'
    fn = ctx.datadir + ctx.basetime.strftime('humid-%Y%m%d.log')
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: 'meas_thp' not in d)


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
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: 'meas_sc' not in d)


def makeDLcurrents(ctx, uubnums):
    """Create LogHandlerFile for currents drawn by UUBNSTR
ctx - context object, used keys: datadir + basetime"""
    prolog = """\
# Currents drawn by UUBs [mA]
# date %s
# columns: timestamp | PowerSup voltage [V] | PowerSup current [A]\
""" % (ctx.basetime.strftime('%Y-%m-%d'))
    prolog += ''.join([' | UUB-%04d.itot' % uubnum
                       for uubnum in uubnums])
    prolog += "\n"
    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
               '{ps_u:6.3f}', '{ps_i:6.3f}']
    logdata += ['{itot_u%04d:5.1f}' % uubnum for uubnum in uubnums]
    formatstr = ' '.join(logdata) + '\n'
    fn = ctx.datadir + ctx.basetime.strftime('currents-%Y%m%d.log')
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          missing_keys={'ps_u': '  ~   ',
                                        'ps_i': '  ~   '},
                          skiprec=lambda d: 'meas_sc' not in d)


def makeDLpedenoise(ctx, uubnum, count=None):
    """Create LogHandlerFile for pedestals and noise
ctx - context object, used keys: datadir + basetime + chans
uubnum - UUB to log"""
    fn = ctx.datadir + ('pedenoise_u%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    prolog = """\
# Pedestals and noise
# UUB #%04d, date %s
# columns: timestamp | meas_point | set_temp""" % (
        uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    if count is not None:
        prolog += " | index"
    for typ, fmt in (('pede', '7.2f'), ('noise', '7.2f')):
        prolog += ''.join([' | %s.ch%d' % (typ, chan)
                           for chan in ctx.chans])
    prolog += '\n'

    if count is None:
        logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                   '{meas_point:4d}', '{set_temp:5.1f}']
        for typ, fmt in (('pede', '7.2f'), ('noise', '7.2f')):
            logdata += ['{%s:%s}' % (item2label(
                functype='N', uubnum=uubnum, chan=chan, typ=typ), fmt)
                        for chan in ctx.chans]
        formatstr = ' '.join(logdata) + '\n'
    else:
        loglines = []
        for ind in range(count):
            logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                       '{meas_point:4d}', '{set_temp:5.1f}', "%03d" % ind]
            for typ, fmt in (('pede', '7.2f'), ('noise', '7.2f')):
                logdata += ['{%s:%s}' % (
                    item2label(functype='N', uubnum=uubnum, chan=chan,
                               typ=typ, index=ind), fmt)
                            for chan in ctx.chans]
            loglines.append(' '.join(logdata) + '\n')
        formatstr = ''.join(loglines)
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: 'meas_noise' not in d)


def makeDLstat(ctx, uubnum, styp):
    """Create LogHandlerFile for staticstics (mean + stdev)
ctx - context object, used keys: datadir + basetime + chans
uubnum - UUB to log
styp - variable to calculate statistics for (e.g. pede or noise)"""
    params = {
        'pede': {'fn': 'pede_u%04d',
                 'prolog': 'Pedestals statistics: mean + stdev',
                 'skiprec': 'meas_noise'},
        'noise': {'fn': 'noise_u%04d',
                  'prolog': 'Noise statistics: mean + stdev',
                  'skiprec': 'meas_noise'}
    }
    assert styp in params.keys()
    p = params[styp]
    fn = ctx.datadir + (p['fn'] % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    prolog = """\
# %s
# UUB #%04d, date %s
# columns: timestamp | meas_point | set_temp""" % (
        p['prolog'], uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
               '{meas_point:4d}', '{set_temp:5.1f}']
    for typ, fmt in ((styp+'mean', '7.2f'), (styp+'stdev', '7.2f')):
        prolog += ''.join([' | %s.ch%d' % (typ, chan) for chan in ctx.chans])
        logdata += ['{%s:%s}' % (item2label(
            functype='N', uubnum=uubnum, chan=chan, typ=typ), fmt)
                    for chan in ctx.chans]
    prolog += '\n'
    formatstr = ' '.join(logdata) + '\n'
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: p['skiprec'] not in d)


def makeDLhsampli(ctx, uubnum, keys):
    """Create LogHandlerFile for halfsine amplitudes
ctx - context object, used keys: datadir + basetime + chans
      afg.params, splitmode
uubnum - UUB to log
keys - voltages and/or splitmodes and/or count"""
    if keys is None:
        keys = {}
    voltages = keys.get('voltages', (None, ))
    splitmodes = keys.get('splitmodes', (None, ))
    if 'count' in keys:
        indices = range(keys['count'])
    else:
        indices = (None, )
    fn = ctx.datadir + ('ampli_uub%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    itemr = {'functype': 'P', 'typ': 'ampli', 'uubnum': uubnum}
    prolog = """\
# Amplitudes of halfsines
# UUB #%04d, date %s
""" % (uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    if 'comment' in keys:
        prolog += "# %s\n" % keys['comment']
    prolog += "# columns: timestamp | meas_point | set_temp | "
    if splitmodes[0] is not None:
        prolog += "splitmode | "
    if voltages[0] is not None:
        prolog += "voltage | "
    if indices[0] is not None:
        prolog += "index | "
    prolog += ' | '.join(['ampli.ch%d' % chan for chan in ctx.chans])
    prolog += '\n'
    loglines = []
    for splitmode in splitmodes:
        for voltage in voltages:
            for ind in indices:
                logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                           '{meas_point:4d}', '{set_temp:5.1f}']
                if splitmode is not None:
                    logdata.append('%d' % splitmode)
                    itemr['splitmode'] = splitmode
                if voltage is not None:
                    logdata.append('%5.3f' % voltage)
                    itemr['voltage'] = voltage
                if ind is not None:
                    logdata.append('%03d' % ind)
                    itemr['index'] = ind
                logdata += [
                    NOTCALC if ctx.notcalc('P', chan, splitmode, voltage)
                    else '{%s:7.2f}' % item2label(itemr, chan=chan)
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
    voltages = keys.get('voltages', (None, ))
    splitmodes = keys.get('splitmodes', (None, ))
    freqs = keys.get('freqs', (ctx.afg.param['freq'], ))
    if 'count' in keys:
        indices = range(keys['count'])
    else:
        indices = (None, )
    fn = ctx.datadir + ('fampli_uub%04d' % uubnum) +\
        ctx.basetime.strftime('-%Y%m%d.log')
    itemr = {'functype': 'F', 'typ': 'fampli', 'uubnum': uubnum}
    prolog = """\
# Amplitudes of sines depending on frequency
# UUB #%04d, date %s
""" % (uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    if 'comment' in keys:
        prolog += "# %s\n" % keys['comment']
    prolog += "# columns: timestamp | meas_point | set_temp | "
    prolog += "flabel | freq [MHz] | "
    if splitmodes[0] is not None:
        prolog += "splitmode | "
    if voltages[0] is not None:
        prolog += "voltage | "
    if indices[0] is not None:
        prolog += "index | "
    prolog += ' | '.join(['fampli.ch%d' % chan for chan in ctx.chans])
    prolog += '\n'
    loglines = []
    for freq in freqs:
        flabel = float2expo(freq, manlength=3)
        itemr['flabel'] = flabel
        for splitmode in splitmodes:
            for voltage in voltages:
                for ind in indices:
                    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                               '{meas_point:4d}', '{set_temp:5.1f}',
                               '%-4s %6.2f' % (flabel, freq/1e6)]
                    if splitmode is not None:
                        logdata.append('%d' % splitmode)
                        itemr['splitmode'] = splitmode
                    if voltage is not None:
                        logdata.append('%5.3f' % voltage)
                        itemr['voltage'] = voltage
                    if ind is not None:
                        logdata.append('%03d' % ind)
                        itemr['index'] = ind
                    logdata += [
                        NOTCALC if ctx.notcalc('F', chan, splitmode, voltage)
                        else '{%s:7.2f}' % item2label(itemr, chan=chan)
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
# columns: timestamp | meas_point | set_temp""" % (
        uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
               '{meas_point:4d}', '{set_temp:5.1f}']
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
# columns: timestamp | meas_point | set_temp | flabel | freq [MHz]""" % (
        uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    for typ in ('fgain', 'flin'):
        prolog += ''.join([' | %s.ch%d' % (typ, chan)
                           for chan in ctx.chans])
    prolog += '\n'
    itemr = {'functype': 'F', 'uubnum': uubnum}
    loglines = []
    for freq in freqs:
        flabel = float2expo(freq, manlength=3)
        itemr['flabel'] = flabel
        logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
                   '{meas_point:4d}', '{set_temp:5.1f}',
                   '%-4s %6.2f' % (flabel, freq/1e6)]
        for typ, fmt in (('fgain', '6.3f'), ('flin', '7.5f')):
            logdata += ['{%s:%s}' % (item2label(itemr, chan=chan, typ=typ),
                                     fmt)
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
# columns: timestamp | meas_point | meas_point """ % (
        uubnum, ctx.basetime.strftime('%Y-%m-%d'))
    prolog += ''.join([' | cutoff.ch%d' % chan for chan in ctx.chans]) + '\n'
    logdata = ['{timestamp:%Y-%m-%dT%H:%M:%S}',
               '{meas_point:4d}', '{set_temp:5.1f}']
    itemr = {'uubnum': uubnum, 'typ': 'cutoff'}
    logdata += ['{%s:5.2f}' % item2label(itemr, chan=chan)
                for chan in ctx.chans]
    formatstr = ' '.join(logdata) + '\n'
    return LogHandlerFile(fn, formatstr, prolog=prolog,
                          skiprec=lambda d: 'meas_freq' not in d)
