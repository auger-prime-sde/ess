#
# ESS procedure - communication

from datetime import datetime, timedelta
import threading
import logging
import json

# ESS stuff
from timer import one_tick
from modbus import Modbus, ModbusError, Binder, BinderSegment, BinderProg

class Chamber(threading.Thread):
    """Thread managing Climate chamber"""

    def __init__(self, port, timer, q_resp):
        """Constructor.
port - serial port to connect
timer - instance of Timer
q_resp - queue to send response"""
        super(Chamber, self).__init__()
        self.timer = timer
        self.q_resp = q_resp
        # check that we are connected to Binder climate chamber
        logger = logging.getLogger('chamber')
        b = None
        try:
            m = Modbus(port)
            logger.info('Opening serial %s', repr(m.ser))
            b = Binder(m)
            b.state()
        except ModbusError:
            m.ser.close()
            logger.exception('Init modbus failed')
            raise
        self.binder = b

    def run(self):
        logger = logging.getLogger('chamber')
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                logger.info('Timer stopped, closing serial')
                self.binder.modbus.ser.close()
                return
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            logger.debug('Chamber event timestamp %s',
                         datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S"))
            if 'binder.state' in flags:
                if flags['binder.state'] is None:
                    logger.info('Stopping program')
                    self.binder.setState(Binder.STATE_BASIC)
                try:
                    progno = int(flags['binder.state'])
                    logger.info('Starting program %d', progno)
                    self.binder.setState(Binder.STATE_PROG, progno)
                except Exception:
                    logger.error('Unknown detail for binder.state: %s',
                                 repr(flags['binder.state']))
            if 'meas.thp' in flags or 'meas.point' in flags:
                logger.debug('Chamber temperature & humidity measurement')
                temperature = self.binder.getActTemp()
                humid = self.binder.getActHumid()
                logger.debug('Done. t = %.2fdeg.C, h = %.2f%%',
                             temperature, humid)
                res = {'timestamp': timestamp,
                       'chamber_temp': temperature,
                       'chamber_humid': humid}
                if 'meas.point' in flags:
                    res['meas_point'] = flags['meas.point']['meas_point']
                self.q_resp.put(res)
            if 'binder.prog' in flags:
                progno, prog = (flags['binder.prog']['progno'],
                                flags['binder.prog']['prog'])
                logger.info('Loading program %d', progno)
                prog.send(self.binder, progno)

class ChamberTicker(threading.Thread):
    """Build BinderProg and ticker from JSON description"""

    def __init__(self, jsonobj, timer, q_resp):
        """Constructor
jsonobj - either json string or json file"""
        super(ChamberTicker, self).__init__()
        self.timer, self.q_resp = timer, q_resp
        self.starttime = self.stoptime = None
        if hasattr(jsonobj, 'read'):
            jso = json.load(jsonobj)
        else:
            jso = json.loads(jsonobj)
        self.macros = jso.get('macros', {})
        self.progno = jso.get('progno', 0)
        self.prog = BinderProg()
        self.time_temp = []
        mps = {}
        temp_prev = None
        t = 0
        for segment in jso['program']:
            dur = segment["duration"]
            temp_end = segment["temperature"]
            if temp_prev is None:  # for the first iteration
                temp_prev = temp_end
                if dur == 0:
                    continue
            assert dur > 0, "Non-positive duration of segment"
            self.prog.seg_temp.append(BinderSegment(temp_prev, dur))
            self.time_temp.append((t, temp_prev))
            # meas point
            if "meas" in segment:
                meas = self._macro(segment["meas"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    flags = self._macro(mp["flags"])
                    mptime = t + offset
                    if offset < 0:
                        mptime += dur
                    mps[mptime] = flags
            t += dur
            temp_prev = temp_end
        self.time_temp.append((t, temp_prev))
        # append the last segment
        self.prog.seg_temp.append(BinderSegment(temp_prev, 1))
        self.meas_points = [(mptime, mps[mptime]) for mptime in sorted(mps)]

    def _macro(self, o):
        if isinstance(o, (str, unicode)):
            return self.macros.get(str(o), o)
        return o

    def run(self):
        logger = logging.getLogger('ChamberTicker')
        timestamp = None
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                logger.info('Timer stopped, stopping ChamberTicker')
                return
            if timestamp == self.timer.timestamp:
                continue   # already processed timestamp
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            if self.starttime is None or self.stoptime < timestamp or (
                    'meas.thp' not in flags and 'meas.point' not in flags):
                continue
            dur = (timestamp - self.starttime).total_seconds()
            if dur < 0:
                continue
            try:
                ind = [p[0] > dur for p in self.time_temp].index(True)
                ((t0, temp0), (t1, temp1)) = self.time_temp[ind-1:ind+1]
                x = float(dur - t0) / (t1 - t0)
                temp = x*temp1 + (1-x)*temp0
            except ValueError:    # not necessary now due to stoptime
                temp = self.time_temp[-1][1]
            res = {'timestamp': timestamp,
                   'set_temp': temp}
            self.q_resp.put(res)

    def loadprog(self, delay=60):
        """Create one_tick ticker to load binder prog and add it to timer"""
        self.timer.add_ticker('binder.prog',
                              one_tick(self.timer.basetime, delay=delay,
                                       detail={'prog': self.prog,
                                               'progno': self.progno}))

    def startprog(self, delay=31):
        """Create ticker for meas.point and binder.state
and add them to timer"""
        starttime = datetime.now() + timedelta(seconds=delay)
        starttime = starttime.replace(second=0, microsecond=0,
                                      minute=starttime.minute+1)
        self.stoptime = starttime + timedelta(seconds=self.time_temp[-1][0])
        self.starttime = starttime
        self.timer.add_ticker('binder.state', one_tick(self.timer.basetime,
                                                       starttime, delay=0,
                                                       detail=self.progno))
        self.timer.add_ticker('meas.point', self.measpoint_tick())

    def measpoint_tick(self):
        """Ticker to provide meas_points"""
        if self.starttime is None:
            raise StopIteration
        offset = (self.starttime - self.timer.basetime).total_seconds()
        mpind = 0
        for t, flags in self.meas_points:
            flags['meas_point'] = mpind
            yield flags.copy(), t + offset
            mpind += 1
