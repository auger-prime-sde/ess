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
        yield (None, val)
        val += interval
        if count > 0:
            count -= 1

def one_tick(basetime, timestamp=None, delay=60, detail=None):
    """Generator of one tick
basetime - datetime of reference basetime
           if None, basetime = timestamp
timestamp - datetime when the tick shall be generated
            now() if None
delay - delay in seconds
"""
    if basetime is None:
        yield detail, delay
        return
    if timestamp is None:
        timestamp = datetime.now()
    delta = int((timestamp - basetime).total_seconds() + 0.999999) + delay
    yield detail, delta

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
                assert name not in self.tickers, "Duplicate ticker " + name
                logger.info('Added ticker ' + name)
                # at least one value must be produced
                detail, nextval = gener.next()
                nextval += offset
                self.tickers[name] = [detail, nextval, gener, offset]
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
            delta = min([t[1] for t in self.tickers.values()])
            newflags = {}
            tickers2del = []
            for name, t in self.tickers.iteritems():
#                logger.debug('ticker iteration: %s: %s', name, repr(t))
                if t[1] == delta:
                    newflags[name] = t[0]
                    try:
                        t[0:2] = t[2].next()  # generate next value
                        t[1] += t[3]          # add offset to delta
                    except StopIteration:
                        # schedule exhausted ticker deletion
                        tickers2del.append(name)
            for name in tickers2del:
                del self.tickers[name]
                logger.info('Exhausted ticker %s removed', name)

            timestamp = self.basetime + timedelta(seconds=delta)
            now = datetime.now()
            if now > timestamp:
                logger.debug('Skipping passed tick %s',
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
