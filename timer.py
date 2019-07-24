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
    assert count is None or isinstance(count, int), \
        "periodic_ticker count must be None or integer"
    while count is None or count > 0:
        yield val, None
        val += interval
        if count is not None:
            count -= 1


def point_ticker(tuples, offset=0, pointname=None, timepoints=None):
    """Generator to provide ticks at points
tuples - list of (time, detail)
offset - offset to add to time
pointname - if not None, add index to flags
            (i.e. flags[pointname] = point index)"""
    for t, flags in tuples:
        if pointname is not None and isinstance(timepoints, dict) \
           and isinstance(flags, dict):
            flags = flags.copy()
            flags[pointname] = timepoints[t]
        yield t + offset, flags


def list_ticker(timelist, offset=0, pointname=None, timepoints=None):
    """Generator to provide ticks at <timelist> times
timelist - list of times to tick
offset - an offset to add to time
pointname - if not None, add index to flags
           (i.e. flags[pointname] = point index)"""
    for t in timelist:
        if pointname is not None and isinstance(timepoints, dict):
            flags = {pointname: timepoints[t]}
        else:
            flags = None
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
    EPS = 0.0001  # guard interval for immediates

    def __init__(self, basetime):
        super(Timer, self).__init__()
        self.basetime = basetime
        self._clear()
        self.stop = threading.Event()
        self.evt = threading.Event()
        self.logger = logging.getLogger('timer')
        self.logger.info('Timer initialized: basetime %s',
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

    def add_immediate(self, name, detail):
        """Schedule an event with name and detail ASAP"""
        self.immediate.append((name, detail))

    def _clear(self):
        """Clear all tickers et al."""
        self.tickers = {}  # name: [nextval, detail, gener, offset]
        self.tickers2add = []
        self.tickers2del = []
        self.immediate = []  # (name: detail)
        self.timestamp = None
        self.flags = {}
        self.timerstop = None   # if Event, set() at the end of program

    def run(self):
        zTimerStop = False
        while not self.stop.is_set():
            # update self.tickers
            while self.tickers2add:
                name, gener, offset = self.tickers2add.pop()
                if name in self.tickers:
                    self.logger.error("Duplicate ticker %s, ignoring", name)
                    continue
                self.logger.info('Added ticker ' + name)
                # at least one value must be produced
                nextval, detail = next(gener)
                nextval += offset
                self.tickers[name] = [nextval, detail, gener, offset]
            while self.tickers2del:
                name = self.tickers2del.pop()
                if name in self.tickers:
                    del self.tickers[name]
                    self.logger.info('Removing ticker ' + name)
                else:
                    self.logger.info('Ticker %s not present for removal', name)

            if not self.tickers and not self.immediate:
                sleep(0.3)
                continue

            # the closest possible delta-time for event
            delta0 = int((datetime.now() - self.basetime).total_seconds() +
                         Timer.EPS + 0.999999)
            # find minimal next values of time delta
            if self.immediate:
                delta = min([delta0] + [t[0] for t in self.tickers.values()])
            else:
                delta = min([t[0] for t in self.tickers.values()])
            newflags = {}
            tickers2del = []
            for name, t in self.tickers.items():
                # self.logger.debug('ticker iteration: %s: %s', name, repr(t))
                if t[0] == delta:
                    newflags[name] = t[1]
                    try:
                        t[0:2] = next(t[2])  # generate next value
                        t[0] += t[3]          # add offset to delta
                    except StopIteration:
                        # schedule exhausted ticker deletion
                        tickers2del.append(name)
            for name in tickers2del:
                del self.tickers[name]
                self.logger.info('Exhausted ticker %s removed', name)

            if delta >= delta0:
                nimmediate = []
                while self.immediate:
                    name, detail = self.immediate.pop(0)
                    if name in newflags:
                        nimmediate.append((name, detail))
                    else:
                        newflags[name] = detail
                self.immediate.extend(nimmediate)

            timestamp = self.basetime + timedelta(seconds=delta)
            now = datetime.now()
            if now > timestamp:
                self.logger.debug(
                    'Skipping passed tick %s',
                    datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S.%f"))
                continue

            if 'stop' in newflags:
                zTimerStop = True
                del newflags['stop']
            # coarse sleep
            sec = (timestamp - now).seconds
            if sec > 2:
                self.logger.debug('coarse sleep %ds', sec-2)
                sleep(sec-2)
            # fine sleep
            delta = timestamp - datetime.now()
            sec = delta.seconds + 0.000001 * delta.microseconds
            self.logger.debug('fine sleep %.6fs', sec)
            sleep(sec)
            if newflags:  # may be empty if only stop was present
                self.logger.debug('event: %s', ', '.join(newflags))
                self.timestamp = timestamp
                self.flags = newflags
                self.evt.set()
                self.evt.clear()

            # self stop
            if zTimerStop:
                self.logger.info('timer self stop')
                if self.timerstop is not None:
                    self.timerstop.set()
                self._clear()
                zTimerStop = False
        self.logger.info('timer.run finished')

    def join(self, timeout=None):
        self.logger.debug('Timer.join')
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
