"""
 ESS procedure
 timer implementation
"""

import threading
import logging
from time import sleep
from datetime import datetime, timedelta


def periodic_ticker(interval, count=None, offset=0):
    """Generator of periodic sequence
interval, offset - generates values offset, offset+interval, offset+2*interval
count - number of values to generate, indefinitely if None
yields (None, value)
"""
    val = offset
    while count is None or count > 0:
        yield val, None
        val += interval
        if count > 0:
            count -= 1


def point_ticker(tuples, offset=0, pointname=None):
    """Generator to provide ticks at points
tuples - list of (time, detail)
offset - offset to add to time
pointname - if not None, add index to flags
            (i.e. flags[pointname] = point index)"""
    pind = 0
    for t, flags in tuples:
        if pointname is not None and isinstance(flags, dict):
            flags = flags.copy()
            flags[pointname] = pind
            pind += 1
        yield t + offset, flags


def list_ticker(timelist, offset=0, pointname=None):
    """Generator to provide ticks at <timelist> times
timelist - list of times to tick
offset - an offset to add to time
pointname - if not None, add index to flags
           (i.e. flags[pointname] = point index)"""
    pind = 0
    for t in timelist:
        flags = {pointname: pind} if pointname is not None else None
        yield t + offset, flags


def one_tick(basetime, timestamp=None, delay=60, detail=None):
    """Generator of one tick
basetime - datetime of reference basetime
           if None, basetime = timestamp
timestamp - datetime when the tick shall be generated
            now() if None
delay - delay in seconds
"""
    if basetime is None:
        yield delay, detail
        return
    if timestamp is None:
        timestamp = datetime.now()
    delta = int((timestamp - basetime).total_seconds() + 0.999999) + delay
    yield delta, detail


class Timer(threading.Thread):
    """Event (periodic/from list) generator"""

    def __init__(self, basetime):
        super(Timer, self).__init__()
        self.basetime = basetime
        self.tickers = {}  # name: [gener, offset, candidate]
        self.tickers2add = []
        self.tickers2del = []
        self.timestamp = None
        self.flags = {}
        self.stop = threading.Event()
        self.evt = threading.Event()
        logger = logging.getLogger('timer')
        logger.info('Timer initialized: basetime %s',
                    datetime.strftime(basetime, "%Y-%m-%d %H:%M:%S.%f"))

    def add_ticker(self, name, gener, offset=0):
        """Add a new ticker to timer
name - an identifier
gener - a generator / tuple / list to produce deltas (in seconds)
        relative to basetime
offset - an offset to basetime (seconds)
"""
        self.tickers2add.append((name, gener, offset))

    def del_ticker(self, name):
        """Remove a ticker"""
        self.tickers2del.append(name)

    def run(self):
        logger = logging.getLogger('timer')
        while not self.stop.is_set():
            # update self.tickers
            while self.tickers2add:
                name, gener, offset = self.tickers2add.pop()
                if name in self.tickers:
                    logger.error("Duplicate ticker %s, ignoring", name)
                    continue
                logger.info('Added ticker ' + name)
                # at least one value must be produced
                nextval, detail = gener.next()
                nextval += offset
                self.tickers[name] = [nextval, detail, gener, offset]
            while self.tickers2del:
                name = self.tickers2del.pop()
                if name in self.tickers:
                    del self.tickers[name]
                    logger.info('Removing ticker ' + name)
                else:
                    logger.info('Ticker %s not present for removal', name)

            if not self.tickers:
                sleep(1)
                continue
            # find minimal next values of time delta
            delta = min([t[0] for t in self.tickers.values()])
            newflags = {}
            tickers2del = []
            for name, t in self.tickers.iteritems():
                # logger.debug('ticker iteration: %s: %s', name, repr(t))
                if t[0] == delta:
                    newflags[name] = t[1]
                    try:
                        t[0:2] = t[2].next()  # generate next value
                        t[0] += t[3]          # add offset to delta
                    except StopIteration:
                        # schedule exhausted ticker deletion
                        tickers2del.append(name)
            for name in tickers2del:
                del self.tickers[name]
                logger.info('Exhausted ticker %s removed', name)

            timestamp = self.basetime + timedelta(seconds=delta)
            now = datetime.now()
            if now > timestamp:
                logger.debug(
                    'Skipping passed tick %s',
                    datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S.%f"))
                continue

            # coarse sleep
            sec = (timestamp - now).seconds
            if sec > 2:
                logger.debug('coarse sleep %ds', sec-2)
                sleep(sec-2)
            # fine sleep
            delta = timestamp - datetime.now()
            sec = delta.seconds + 0.000001 * delta.microseconds
            logger.debug('fine sleep %.6fs', sec)
            sleep(sec)
            logger.debug('event')
            self.timestamp = timestamp
            self.flags = newflags
            self.evt.set()
            self.evt.clear()
        logger.info('timer.run finished')

    def join(self, timeout=None):
        logger = logging.getLogger('timer')
        logger.debug('Timer.join')
        self.stop.set()   # stop run() and inform listeners that we finish
        self.flags = {}
        self.evt.set()    # trigger all listeners
        super(Timer, self).join(timeout)


class EvtDisp(threading.Thread):
    """Display event in the timer"""
    def __init__(self, timer):
        self.timer = timer
        super(EvtDisp, self).__init__()

    def run(self):
        logger = logging.getLogger('EvtDisp')
        timestamp = None
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                logger.info('Timer stopped, stopping EvtDisp')
                return
            if timestamp == self.timer.timestamp:
                continue   # already processed timestamp
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            logger.debug('timestamp: %s, flags: %s',
                         timestamp.strftime('%Y-%m-%dT%H:%M:%S'), repr(flags))
