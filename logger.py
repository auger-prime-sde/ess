"""
 ESS procedure
 logging
"""

import logging
import string
import threading
from datetime import datetime, timedelta
from Queue import Empty

def skiprec_MP(d):
    """Check if a record should be skipped according to presence
 of meas. point
d - dictionary where 'meas_point' is looked up
return boolean"""
    return 'meas_point' not in d

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
        self.filters = []

    def write_rec(self, d):
        """Write one record to log
d - dictionary key: value"""
        if self.skiprec is not None and self.skiprec(d):
            return
        for f in self.filters:
            d = f(d)
        record = self.formatter.format(self.formatstr, **d)
        self.f.write(record)
        self.f.flush()

    def __del__(self):
        self.f.close()


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
        # handlers to be added manually !
        self.handlers = []
        self.records = {}
        self.stop = threading.Event()

    def run(self):
        logger = logging.getLogger('logger')
        last_ts = datetime(2016, 1, 1)  # minus infinity
        while not self.stop.is_set():
            if self.records:
                tend = min(self.records.iterkeys())
            else:
                tend = datetime.now()
            tend += timedelta(seconds=self.timeout)
            # logger.debug('tend = %s' %
            # datetime.strftime(tend, "%Y-%m-%d %H:%M:%S"))
            # read from queue until some record is timeouted
            while datetime.now() < tend and not self.stop.is_set():
                try:
                    timeout = (tend - datetime.now()).total_seconds()
                    # logger.debug('timeout = %.6f' % timeout)
                    newrec = self.q_resp.get(True, timeout)
                    try:
                        ts = newrec.pop('timestamp')
                    except AttributeError:
                        logger.debug('Wrong record: %s', repr(newrec))
                        continue
                    if ts in self.records:
                        self.records[ts].update(newrec)
                    elif ts > last_ts:  # add only ts after the last written
                        logger.debug('Added new record %s',
                                     datetime.strftime(ts, "%Y-%m-%d %H:%M:%S"))
                        self.records[ts] = newrec
                    else:
                        logger.info('Discarding an old record %s',
                                    datetime.strftime(ts, "%Y-%m-%d %H:%M:%S"))
                except Empty:
                    # logger.debug('q_resp.get() timeout')
                    pass
            # process timeouted records
            texp = datetime.now() - timedelta(seconds=self.timeout)
            expts = [ts for ts in self.records.iterkeys() if ts < texp]
            for ts in sorted(expts):
                logger.debug('write rec for ts = %s',
                             datetime.strftime(ts, "%Y-%m-%d %H:%M:%S"))
                if ts > last_ts:
                    last_ts = ts
                rec = self.records.pop(ts)
                rec['timestamp'] = ts
                # logger.debug('Rec written to handlers: %s', repr(rec))
                for h in self.handlers:
                    h.write_rec(rec)
        logger.info('run() finished, deleting handlers')
        for h in self.handlers:
            h.__del__()
        self.handlers = None
#        del self.handlers[:]

    def join(self, timeout=None):
        logging.getLogger('logger').debug('DataLogger.join')
        self.stop.set()   # stop run()
        super(DataLogger, self).join(timeout)

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

        
