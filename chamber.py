#
# ESS procedure - communication

from datetime import datetime, timedelta
import re
import threading
import logging
import json

# ESS stuff
from timer import point_ticker, list_ticker, one_tick
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
        self.time_humid = []
        mps = {}   # measurement pulses, {time: flags}
        mfs = {}   # measurement freqs, {time: flags}
        tps = []   # test points: [time]
        pps = {}   # power operations: {time: kwargs}
        temp_prev = None
        humid_prev = None
        time_temp_prev = time_humid_prev = t = 0
        for segment in jso['program']:
            dur = segment["duration"]
            if 'temperature' in segment:
                temp_end = segment["temperature"]
                dur_temp = t + dur - time_temp_prev
                if temp_prev is None:
                    temp_prev = temp_end
                if dur_temp > 0:
                    seg = BinderSegment(temp_prev, dur_temp)
                    self.prog.seg_temp.append(seg)
                    self.time_temp.append((time_temp_prev, temp_prev))
                temp_prev = temp_end
                time_temp_prev += dur_temp
            if 'humidity' in segment:
                humid_end = segment["humidity"]
                dur_humid = t + dur - time_humid_prev
                if humid_prev is None:
                    humid_prev = humid_end
                if dur_humid > 0:
                    seg = BinderSegment(humid_prev, dur_humid)
                    self.prog.seg_humid.append(seg)
                    self.time_humid.append((time_humid_prev, humid_prev))
                humid_prev = humid_end
                time_humid_prev += dur_humid
            # meas points
            if "meas.pulse" in segment:
                meas = self._macro(segment["meas.pulse"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    flags = self._macro(mp["flags"])
                    mptime = t + offset
                    if offset < 0:
                        mptime += dur
                    mps[mptime] = flags
            if "meas.freq" in segment:
                meas = self._macro(segment["meas.freq"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    flags = self._macro(mp["flags"])
                    mptime = t + offset
                    if offset < 0:
                        mptime += dur
                    mfs[mptime] = flags
            # power and test
            if "power" in segment:
                power = self._macro(segment["power"])
                for pp in power:
                    pp = self._macro(pp)
                    offset = self._macro(pp["offset"])
                    ptime = t + offset
                    if offset < 0:
                        ptime += dur
                    if "test" in pp:
                        tps.append(ptime)
                    # kwargs for PowerSupply.config()
                    # e.g. {'ch1': (12.0, None, True, False)}
                    kwargs = {}
                    for chan in pp:
                        if re.match(r'^ch\d$', chan) is None:
                            continue
                        args = self._macro(pp[chan])
                        if not all([v is None for v in args]):
                            kwargs[chan] = args
                    if kwargs:
                        pps[ptime] = kwargs
            t += dur
        self.progdur = t
        # append the last segment
        if temp_prev is not None:
            self.time_temp.append((t, temp_prev))
            self.prog.seg_temp.append(BinderSegment(temp_prev, 1))
        if humid_prev is not None:
            self.time_humid.append((t, humid_prev))
            self.prog.seg_humid.append(BinderSegment(humid_prev, 1))
        self.meas_pulses = [(mptime, mps[mptime]) for mptime in sorted(mps)]
        self.meas_freqs = [(mptime, mfs[mptime]) for mptime in sorted(mfs)]
        self.power_points = [(ptime, pps[ptime]) for ptime in sorted(pps)]
        self.test_points = sorted(tps)

    def _macro(self, o):
        if isinstance(o, (str, unicode)):
            return self.macros.get(str(o), o)
        return o

    def polyline(self, t, timevalues):
        """Providing timevalues as a list of tuples (time, value)
return polyline approximation at the time t"""
        try:
            ind = [p[0] > t for p in timevalues].index(True)
            ((t0, val0), (t1, val1)) = timevalues[ind-1:ind+1]
            x = float(t - t0) / (t1 - t0)
            val = x*val1 + (1-x)*val0
        except ValueError:    # not necessary now due to stoptime
            val = timevalues[-1][1]
        return val

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
                    'meas.thp' not in flags and
                    'meas.sc' not in flags and
                    'meas.pulse' not in flags and
                    'meas.freq' not in flags):
                continue
            dur = (timestamp - self.starttime).total_seconds()
            if dur < 0:
                continue
            res = {'timestamp': timestamp}
            if self.time_temp:
                res['set_temp'] = self.polyline(dur, self.time_temp)
            if self.time_humid:
                res['set_humid'] = self.polyline(dur, self.time_humid)
            if len(res) > 1:
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
        self.stoptime = starttime + timedelta(seconds=self.progdur)
        self.starttime = starttime
        self.timer.add_ticker('binder.state', one_tick(self.timer.basetime,
                                                       starttime, delay=0,
                                                       detail=self.progno))
        offset = (self.starttime - self.timer.basetime).total_seconds()
        if self.meas_pulses:
            self.timer.add_ticker('meas.pulse',
                                  point_ticker(self.meas_pulses, offset,
                                               'meas_pulse_point'))
        if self.meas_freqs:
            self.timer.add_ticker('meas.freq',
                                  point_ticker(self.meas_freqs, offset,
                                               'meas_freq_point'))
        if self.power_points:
            self.timer.add_ticker('power',
                                  point_ticker(self.power_points, offset))
        if self.test_points:
            self.timer.add_ticker('power.test',
                                  list_ticker(self.test_points, offset,
                                              'test_point'))
