#
# ESS procedure - communication

from datetime import datetime, timedelta
import re
import threading
import logging
import json

# ESS stuff
from timer import point_ticker, list_ticker
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
            if all([name not in flags
                    for name in ('binder.state', 'binder.prog', 'meas.sc',
                                 'meas.ramp', 'meas.noise', 'meas.iv',
                                 'meas.thp', 'meas.pulse', 'meas.freq')]):
                continue
            logger.debug('Chamber event timestamp %s',
                         datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S"))
            if 'binder.state' in flags:
                if flags['binder.state'] is None:
                    logger.info('Stopping program')
                    self.binder.setState(Binder.STATE_BASIC)
                else:
                    try:
                        progno = int(flags['binder.state'])
                        logger.info('Starting program %d', progno)
                        self.binder.setState(Binder.STATE_PROG, progno)
                    except Exception:
                        logger.error('Unknown detail for binder.state: %s',
                                     repr(flags['binder.state']))
            if any([name in flags
                    for name in ('meas.sc', 'meas.thp',
                                 'meas.ramp', 'meas.noise', 'meas.iv',
                                 'meas.pulse', 'meas.freq')]):
                logger.debug('Chamber temperature & humidity measurement')
                temperature = self.binder.getActTemp()
                humid = self.binder.getActHumid()
                logger.debug('Done. t = %.2fdeg.C, h = %.2f%%',
                             temperature, humid)
                res = {'timestamp': timestamp,
                       'chamber_temp': temperature,
                       'chamber_humid': humid}
                self.q_resp.put(res)
            if 'binder.prog' in flags:
                progno, prog = (flags['binder.prog']['progno'],
                                flags['binder.prog']['prog'])
                logger.info('Loading program %d', progno)
                prog.send(self.binder, progno)


class ESSprogram(threading.Thread):
    """Build BinderProg and ticker from JSON description"""

    def __init__(self, jsonobj, timer, q_resp):
        """Constructor
jsonobj - either json string or json file"""
        super(ESSprogram, self).__init__()
        self.timer, self.q_resp = timer, q_resp
        self.starttime = self.stoptime = None
        if hasattr(jsonobj, 'read'):
            jso = json.load(jsonobj)
        else:
            jso = json.loads(jsonobj)
        self.macros = jso.get('macros', {})
        self.progno = jso.get('progno', 0)
        self.load = jso.get('load', False)
        self.prog = BinderProg()
        self.time_temp = []
        self.time_humid = []
        mrs = []   # measurement ADC ramp: [time]
        mns = []   # measurement noise: [time]
        mps = {}   # measurement pulses, {time: flags}
        mfs = {}   # measurement freqs, {time: flags}
        ivs = []   # measurement power supply voltage/current: [time]
        tps = []   # test points: [time]
        pps = {}   # power operations: {time: kwargs}
        lis = {}   # logins {time: flags}, flags - None or list of UUBnums
        los = {}   # logouts {time: flags}, flags - None or list of UUBnums
        temp_prev = None
        humid_prev = None
        operc = 0
        t = 0
        for segment in jso['program']:
            dur = segment["duration"]
            # temperature & operc
            temp_end = segment.get("temperature", temp_prev)
            if temp_prev is None and temp_end is not None:
                temp_prev = temp_end
                self.time_temp.append((0, temp_prev))
                if t > 0:
                    seg = BinderSegment(temp_prev, t)
                    self.prog.seg_temp.append(seg)
                    self.time_temp.append((t, temp_prev))
            if temp_prev is not None and dur > 0:
                operc = segment.get("operc", operc)
                seg = BinderSegment(temp_prev, dur, operc=operc)
                self.prog.seg_temp.append(seg)
                self.time_temp.append((t + dur, temp_end))
            temp_prev = temp_end
            # humidity
            humid_end = segment.get("humidity", humid_prev)
            if humid_prev is None and humid_end is not None:
                humid_prev = humid_end
                self.time_humid.append((0, humid_prev))
                if t > 0:
                    seg = BinderSegment(humid_prev, t)
                    self.prog.seg_humid.append(seg)
                    self.time_humid.append((t, humid_prev))
            if humid_prev is not None and dur > 0:
                seg = BinderSegment(humid_prev, dur)
                self.prog.seg_humid.append(seg)
                self.time_humid.append((t + dur, humid_end))
            humid_prev = humid_end
            # meas points
            if "meas.ramp" in segment:
                meas = self._macro(segment["meas.ramp"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    mptime = t + offset
                    if offset < 0:
                        mptime += dur
                    mrs.append(mptime)
            if "meas.noise" in segment:
                meas = self._macro(segment["meas.noise"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    mptime = t + offset
                    if offset < 0:
                        mptime += dur
                    mns.append(mptime)
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
            if "meas.iv" in segment:
                meas = self._macro(segment["meas.iv"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    mptime = t + offset
                    if offset < 0:
                        mptime += dur
                    ivs.append(mptime)
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
                    if "login" in pp:
                        lis[ptime] = self._macro(pp["login"])
                    if "logout" in pp:
                        los[ptime] = self._macro(pp["logout"])
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
        # append the last segments
        if temp_prev is not None:
            self.prog.seg_temp.append(BinderSegment(temp_prev, 1))
        if humid_prev is not None:
            self.prog.seg_humid.append(BinderSegment(humid_prev, 1))
        self.meas_ramps = sorted(mrs)
        self.meas_noises = sorted(mns)
        self.meas_pulses = [(mptime, mps[mptime]) for mptime in sorted(mps)]
        self.meas_freqs = [(mptime, mfs[mptime]) for mptime in sorted(mfs)]
        self.meas_ivs = sorted(ivs)
        self.power_points = [(ptime, pps[ptime]) for ptime in sorted(pps)]
        self.test_points = sorted(tps)
        self.logins = [(ptime, lis[ptime]) for ptime in sorted(lis)]
        self.logouts = [(ptime, los[ptime]) for ptime in sorted(los)]
        self.timepoints = {ptime: pind for pind, ptime in enumerate(
            sorted(set(mrs, mns, mps.keys(), mfs.keys(), ivs, pps.keys(),
                       tps, lis.keys(), los.keys())))}

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
        logger = logging.getLogger('ESSprogram')
        if self.load:
            self.timer.add_immediate('binder.prog', {'prog': self.prog,
                                                     'progno': self.progno})
        timestamp = None
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                logger.info('Timer stopped, stopping ESSprogram')
                return
            if timestamp == self.timer.timestamp:
                continue   # already processed timestamp
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            if self.starttime is None or self.stoptime < timestamp or all(
                    [name not in flags
                     for name in ('meas.sc', 'meas.thp',
                                  'meas.ramp', 'meas.noise', 'meas.iv',
                                  'meas.pulse', 'meas.freq')]):
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

    def startprog(self, delay=31):
        """Create tickers for meas.*, power.*, telnet.* and binder.state
and add them to timer"""
        starttime = datetime.now() + timedelta(seconds=60+delay)
        starttime = starttime.replace(second=0, microsecond=0)
        self.stoptime = starttime + timedelta(seconds=self.progdur)
        self.starttime = starttime
        message = starttime.strftime('starting ESS program at %H:%M, duration')
        message += ' %d:%02d' % (self.progdur / 60, self.progdur % 60)
        logging.getLogger('ESSprogram').info(message)
        offset = (self.starttime - self.timer.basetime).total_seconds()
        # start and stop binder program
        self.timer.add_ticker('binder.state',
                              point_ticker(((0, self.progno),
                                            (self.progdur, None)), offset))
        if self.meas_ramps:
            self.timer.add_ticker('meas.ramp',
                                  list_ticker(self.meas_ramps, offset,
                                              'meas_ramp_point',
                                              self.timepoints))
        if self.meas_noises:
            self.timer.add_ticker('meas.noise',
                                  list_ticker(self.meas_noises, offset,
                                              'meas_noise_point',
                                              self.timepoints))
        if self.meas_pulses:
            self.timer.add_ticker('meas.pulse',
                                  point_ticker(self.meas_pulses, offset,
                                               'meas_pulse_point',
                                               self.timepoints))
        if self.meas_freqs:
            self.timer.add_ticker('meas.freq',
                                  point_ticker(self.meas_freqs, offset,
                                               'meas_freq_point',
                                               self.timepoints))
        if self.meas_ivs:
            self.timer.add_ticker('meas.iv',
                                  list_ticker(self.meas_ivs, offset,
                                              'meas_iv_point',
                                              self.timepoints))
        if self.power_points:
            self.timer.add_ticker('power',
                                  point_ticker(self.power_points, offset))
        if self.logins:
            self.timer.add_ticker('power.login',
                                  point_ticker(self.logins, offset))
        if self.logouts:
            self.timer.add_ticker('power.logout',
                                  point_ticker(self.logouts, offset))
        if self.test_points:
            self.timer.add_ticker('power.test',
                                  list_ticker(self.test_points, offset,
                                              'test_point',
                                              self.timepoints))
