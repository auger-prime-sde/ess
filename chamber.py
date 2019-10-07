#
# ESS procedure - communication

from datetime import datetime, timedelta
import re
import threading
import logging
import json

# ESS stuff
from timer import one_tick, point_ticker, list_ticker
from modbus import Modbus, ModbusError, Binder, BinderSegment, BinderProg
from threadid import syscall, SYS_gettid


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
        self.logger = logging.getLogger('chamber')
        b = None
        try:
            m = Modbus(port)
            self.logger.info('Opening serial %s', repr(m.ser))
            b = Binder(m)
            b.state()
        except ModbusError:
            m.ser.close()
            self.logger.exception('Init modbus failed')
            raise
        self.binder = b

    def run(self):
        tid = syscall(SYS_gettid)
        self.logger.debug('run start, name %s, tid %d', self.name, tid)
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                self.logger.info('Timer stopped, closing serial')
                self.binder.modbus.ser.close()
                self.binder = None
                return
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            if all([name not in flags
                    for name in ('binder.state', 'binder.prog', 'meas.sc',
                                 'meas.ramp', 'meas.noise', 'meas.iv',
                                 'meas.thp', 'meas.pulse', 'meas.freq')]):
                continue
            self.logger.debug('Chamber event timestamp %s',
                         datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S"))
            if 'binder.state' in flags:
                if flags['binder.state'] is None:
                    self.logger.info('Stopping program')
                    self.binder.setState(Binder.STATE_BASIC)
                else:
                    try:
                        progno = int(flags['binder.state'])
                        self.logger.info('Starting program %d', progno)
                        self.binder.setState(Binder.STATE_PROG, progno)
                    except Exception:
                        self.logger.error('Unknown detail for binder.state: %s',
                                     repr(flags['binder.state']))
            if any([name in flags
                    for name in ('meas.sc', 'meas.thp',
                                 'meas.ramp', 'meas.noise', 'meas.iv',
                                 'meas.pulse', 'meas.freq')]):
                self.logger.debug('Chamber temperature & humidity measurement')
                temperature = self.binder.getActTemp()
                humid = self.binder.getActHumid()
                self.logger.debug('Done. t = %.2fdeg.C, h = %.2f%%',
                             temperature, humid)
                res = {'timestamp': timestamp,
                       'chamber_temp': temperature,
                       'chamber_humid': humid}
                self.q_resp.put(res)
            if 'binder.prog' in flags:
                progno, prog = (flags['binder.prog']['progno'],
                                flags['binder.prog']['prog'])
                self.logger.info('Loading program %d', progno)
                prog.send(self.binder, progno)


class ESSprogram(threading.Thread):
    """Build BinderProg and ticker from JSON description"""

    class _points(object):
        """Container for measurement poits et al."""
        def __init__(self):
            self.mrs = {}   # measurement ADC ramp: {time: flags}
            self.mns = {}   # measurement noise: {time: flags}
            self.mps = {}   # measurement pulses, {time: flags}
            self.mfs = {}   # measurement freqs, {time: flags}
            self.ivs = {}   # measurement power supply voltage/current: [time]
            self.tps = []   # test points: [time]
            self.pps = {}   # power operations: {time: kwargs}
            # flags for lis/los/cms: None or list of UUBnums
            self.lis = {}   # logins {time: flags}
            self.los = {}   # logouts {time: flags}
            self.cms = {}   # telnet cmds {time: flags}
            self.fls = {}   # flir operations {time: flags}
            self.lto = {}   # log_timeout: {time: log_timeout value}
            self.t = 0      # time
            self.time_temp = []
            self.time_humid = []

        def update(self, otherpoints):
            """Update current points from otherpoints"""
            # update time_temp and time_humid
            for timelist in ('time_temp', 'time_humid'):
                self.__dict__[timelist].extend(
                    [(t + self.t, val)
                     for t, val in otherpoints.__dict__[timelist]])
            # { time: flags } dictionaries
            for dname in ('mrs', 'mns', 'mps', 'mfs', 'ivs', 'pps',
                          'lis', 'los', 'cms', 'fls', 'lto'):
                self.__dict__[dname].update(
                    {t + self.t: val
                     for t, val in otherpoints.__dict__[dname].items()})
            self.tps.extend([t + self.t for t in otherpoints.tps])

        def keys(self):
            """Return all time points"""
            keys = self.tps
            for dname in ('mrs', 'mns', 'mps', 'mfs', 'ivs', 'pps',
                          'lis', 'los', 'cms', 'fls', 'lto'):
                keys.extend(list(self.__dict__[dname].keys()))
            return sorted(set(keys))

    def __init__(self, jsonobj, timer, q_resp, essprog_macros=None):
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
        if essprog_macros is not None:
            self.macros.update(essprog_macros)
        self.progno = jso.get('progno', 0)
        self.load = jso.get('load', False)
        self.prog = BinderProg()
        temp_prev = None
        humid_prev = None
        temp_seg = None
        humid_seg = None
        operc = 0
        gp = ESSprogram._points()  # global points
        cp = None                  # points in cycle
        numrepeat = None           # number to repeat
        ap = gp                    # actual points
        self.timerstop = None
        for segment in jso['program']:
            if 'num_repeat' in segment:  # start cycle
                assert cp is None, "Nested cycle"
                numrepeat = segment['num_repeat']
                assert isinstance(numrepeat, int) and numrepeat > 1
                if temp_prev is not None:
                    temp_seg = len(self.prog.seg_temp)
                if humid_prev is not None:
                    humid_seg = len(self.prog.seg_humid)
                cp = ESSprogram._points()
                ap = cp
                continue
            elif segment == 'endcycle':
                assert cp is not None, "End cycle while not in cycle"
                if temp_seg is not None:
                    self.prog.seg_temp[-1].numjump = numrepeat-1
                    self.prog.seg_temp[-1].segjump = temp_seg
                if humid_seg is not None:
                    self.prog.seg_humid[-1].numjump = numrepeat-1
                    self.prog.seg_humid[-1].segjump = humid_seg
                for i in range(numrepeat):
                    gp.update(cp)
                    gp.t += cp.t
                temp_seg, humid_seg, cp, numrepeat = None, None, None, None
                ap = gp
                continue
            elif 'stop' in segment:  # stop timer after a delay
                assert self.timerstop is None, "Double stop timer"
                assert cp is None, "Stop timer while in cycle"
                self.timerstop = gp.t + int(segment['stop'])
                continue
            else:
                assert self.timerstop is None, "Segment after stop"
            dur = segment["duration"]
            # temperature & operc
            temp_end = segment.get("temperature", temp_prev)
            operc = segment.get("operc", operc)
            if temp_prev is None and temp_end is not None:
                temp_prev = temp_end
                gp.time_temp.append((0, temp_prev))
                if gp.t > 0:
                    seg = BinderSegment(temp_prev, gp.t, operc=operc)
                    self.prog.seg_temp.append(seg)
                    gp.time_temp.append((gp.t, temp_prev))
                if cp is not None and cp.t > 0:
                    temp_seg = len(self.prog.seg_temp)
                    seg = BinderSegment(temp_prev, cp.t, operc=operc)
                    self.prog.seg_temp.append(seg)
                    cp.time_temp.append((cp.t, temp_prev))
            if temp_prev is not None and dur > 0:
                seg = BinderSegment(temp_prev, dur, operc=operc)
                self.prog.seg_temp.append(seg)
                ap.time_temp.append((ap.t + dur, temp_end))
            temp_prev = temp_end
            # humidity
            humid_end = segment.get("humidity", humid_prev)
            if humid_prev is None and humid_end is not None:
                humid_prev = humid_end
                gp.time_humid.append((0, humid_prev))
                if gp.t > 0:
                    seg = BinderSegment(humid_prev, gp.t)
                    self.prog.seg_humid.append(seg)
                    gp.time_humid.append((gp.t, humid_prev))
                if cp is not None and cp.t > 0:
                    humid_seg = len(self.prog.seg_humid)
                    seg = BinderSegment(humid_prev, cp.t)
                    self.prog.seg_humid.append(seg)
                    cp.time_humid.append((cp.t, humid_prev))
            if humid_prev is not None and dur > 0:
                seg = BinderSegment(humid_prev, dur)
                self.prog.seg_humid.append(seg)
                ap.time_humid.append((ap.t + dur, humid_end))
            humid_prev = humid_end
            # meas points
            if "meas.ramp" in segment:
                meas = self._macro(segment["meas.ramp"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    mptime = ap.t + offset
                    if offset < 0:
                        mptime += dur
                    if 'log_timeout' in mp:
                        log_timeout = int(self._macro(mp['log_timeout']))
                        if mptime not in ap.lto or \
                           log_timeout > ap.lto[mptime]:
                            ap.lto[mptime] = log_timeout
                    ap.mrs[mptime] = {key: self._macro(mp[key])
                                      for key in ('db', 'count')
                                      if key in mp}
            if "meas.noise" in segment:
                meas = self._macro(segment["meas.noise"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    mptime = ap.t + offset
                    if offset < 0:
                        mptime += dur
                    if 'log_timeout' in mp:
                        log_timeout = int(self._macro(mp['log_timeout']))
                        if mptime not in ap.lto or \
                           log_timeout > ap.lto[mptime]:
                            ap.lto[mptime] = log_timeout
                    ap.mns[mptime] = {key: self._macro(mp[key])
                                      for key in ('db', 'count')
                                      if key in mp}
            if "meas.pulse" in segment:
                meas = self._macro(segment["meas.pulse"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    mptime = ap.t + offset
                    if offset < 0:
                        mptime += dur
                    if 'log_timeout' in mp:
                        log_timeout = int(self._macro(mp['log_timeout']))
                        if mptime not in ap.lto or \
                           log_timeout > ap.lto[mptime]:
                            ap.lto[mptime] = log_timeout
                    if 'flags' in mp:
                        flags = self._macro(mp["flags"])
                    else:
                        flags = {key: self._macro(mp[key])
                                 for key in ('db', 'voltages', 'splitmodes',
                                             'count')
                                 if key in mp}
                    ap.mps[mptime] = flags
            if "meas.freq" in segment:
                meas = self._macro(segment["meas.freq"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    mptime = ap.t + offset
                    if offset < 0:
                        mptime += dur
                    if 'log_timeout' in mp:
                        log_timeout = int(self._macro(mp['log_timeout']))
                        if mptime not in ap.lto or \
                           log_timeout > ap.lto[mptime]:
                            ap.lto[mptime] = log_timeout
                    if 'flags' in mp:
                        flags = self._macro(mp["flags"])
                    else:
                        flags = {key: self._macro(mp[key])
                                 for key in ('db', 'voltages', 'splitmodes',
                                             'freqs', 'count')
                                 if key in mp}
                    ap.mfs[mptime] = flags
            if "meas.iv" in segment:
                meas = self._macro(segment["meas.iv"])
                for mp in meas:
                    mp = self._macro(mp)
                    offset = self._macro(mp["offset"])
                    flags = self._macro(mp["flags"])
                    mptime = ap.t + offset
                    if offset < 0:
                        mptime += dur
                    if 'log_timeout' in mp:
                        log_timeout = int(self._macro(mp['log_timeout']))
                        if mptime not in ap.lto or \
                           log_timeout > ap.lto[mptime]:
                            ap.lto[mptime] = log_timeout
                    ap.ivs[mptime] = flags
            # power and test
            if "power" in segment:
                power = self._macro(segment["power"])
                for pp in power:
                    pp = self._macro(pp)
                    offset = self._macro(pp["offset"])
                    ptime = ap.t + offset
                    if offset < 0:
                        ptime += dur
                    if "test" in pp:
                        ap.tps.append(ptime)
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
                        ap.pps[ptime] = kwargs
            # telnet
            if "telnet" in segment:
                telnet = self._macro(segment["telnet"])
                for tp in telnet:
                    tp = self._macro(tp)
                    offset = self._macro(tp["offset"])
                    ttime = ap.t + offset
                    if offset < 0:
                        ttime += dur
                    if 'log_timeout' in tp:
                        log_timeout = int(self._macro(tp['log_timeout']))
                        if mptime not in ap.lto or \
                           log_timeout > ap.lto[mptime]:
                            ap.lto[mptime] = log_timeout
                    if "login" in tp:
                        ap.lis[ttime] = self._macro(tp["login"])
                    if "logout" in tp:
                        ap.los[ttime] = self._macro(tp["logout"])
                    if "cmds" in tp:
                        cmdlist = [str(self._macro(cmd))
                                   for cmd in self._macro(tp["cmds"])]
                        if "cmds.uubs" in tp:
                            uubnums = self._macro(tp["cmds.uubs"])
                        else:
                            uubnums = None
                        ap.cms[ttime] = {"cmdlist": cmdlist,
                                         "uubnums": uubnums}
            # FLIR
            if 'flir' in segment:
                flir = self._macro(segment["flir"])
                for fp in flir:
                    fp = self._macro(fp)
                    offset = self._macro(fp["offset"])
                    ftime = ap.t + offset
                    if offset < 0:
                        ftime += dur
                    if 'log_timeout' in fp:
                        log_timeout = int(self._macro(fp['log_timeout']))
                        if mptime not in ap.lto or \
                           log_timeout > ap.lto[mptime]:
                            ap.lto[mptime] = log_timeout
                    flags = {key: self._macro(fp[key])
                             for key in ('imagename', 'attname', 'description',
                                         'snapshot', 'download', 'delete')
                             if key in fp}
                    if 'snapshot' in flags:
                        assert 'imagename' in flags, \
                            "Imagename mandatory for snapshot"
                    ap.fls[ftime] = flags
            ap.t += dur
        assert cp is None, "Unfinished cycle"
        self.progdur = gp.t
        self.time_temp = gp.time_temp
        self.time_humid = gp.time_humid
        # append the last segments
        if temp_prev is not None:
            self.prog.seg_temp.append(BinderSegment(temp_prev, 1))
        if humid_prev is not None:
            self.prog.seg_humid.append(BinderSegment(humid_prev, 1))
        self.meas_ramps = [(mptime, gp.mrs[mptime])
                           for mptime in sorted(gp.mrs)]
        self.meas_noises = [(mptime, gp.mns[mptime])
                            for mptime in sorted(gp.mns)]
        self.meas_pulses = [(mptime, gp.mps[mptime])
                            for mptime in sorted(gp.mps)]
        self.meas_freqs = [(mptime, gp.mfs[mptime])
                           for mptime in sorted(gp.mfs)]
        self.meas_ivs = [(mptime, gp.ivs[mptime]) for mptime in sorted(gp.ivs)]
        self.power_points = [(ptime, gp.pps[ptime])
                             for ptime in sorted(gp.pps)]
        self.test_points = sorted(gp.tps)
        self.logins = [(ttime, gp.lis[ttime]) for ttime in sorted(gp.lis)]
        self.logouts = [(ttime, gp.los[ttime]) for ttime in sorted(gp.los)]
        self.cmds = [(ttime, gp.cms[ttime]) for ttime in sorted(gp.cms)]
        self.flirs = [(ftime, gp.fls[ftime]) for ftime in sorted(gp.fls)]
        self.lto_touts = [(ltime, gp.lto[ltime]) for ltime in sorted(gp.lto)]
        self.lto_touts.append((None, None))  # sentinel
        self.timepoints = {ptime: pind for pind, ptime in enumerate(gp.keys())}

    def _macro(self, o):
        if isinstance(o, str):
            return self.macros.get(str(o), o)
        return o

    def polyline(self, t, timevalues):
        """Providing timevalues as a list of tuples (time, value)
return polyline approximation at the time t"""
        try:
            ind = [p[0] > t for p in timevalues].index(True)
            ((t0, val0), (t1, val1)) = timevalues[ind-1:ind+1]
            x = (t - t0) / (t1 - t0)
            val = x*val1 + (1-x)*val0
        except ValueError:    # not necessary now due to stoptime
            val = timevalues[-1][1]
        return val

    def run(self):
        logger = logging.getLogger('ESSprogram')
        tid = syscall(SYS_gettid)
        logger.debug('run start, name %s, tid %d', self.name, tid)
        if self.load:
            self.timer.add_immediate('binder.prog', {'prog': self.prog,
                                                     'progno': self.progno})
        timestamp = None
        ltime, lto = self.lto_touts.pop(0)
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                logger.info('Timer stopped, stopping ESSprogram')
                return
            if timestamp == self.timer.timestamp:
                continue   # already processed timestamp
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            if self.starttime is None or self.stoptime < timestamp:
                # or all(
                #     [name not in flags
                #      for name in ('meas.sc', 'meas.thp',
                #                   'meas.ramp', 'meas.noise', 'meas.iv',
                #                   'meas.pulse', 'meas.freq')]):
                dur = None
            else:
                dur = (timestamp - self.starttime).total_seconds()
                if dur < 0:
                    dur = None
            res = {'timestamp': timestamp, 'rel_time': dur}
            if ltime is not None and dur is not None and ltime <= dur:
                if ltime == dur:
                    res['log_timeout'] = lto
                ltime, lto = self.lto_touts.pop(0)
            for name in ('meas.ramp', 'meas.noise', 'meas.pulse', 'meas.freq'):
                if name in flags:
                    mname = name.split('.')[1]
                    res['meas_' + mname] = True
                    if 'db' in flags[name]:
                        res['db_' + mname] = flags[name]['db']
            if dur in self.timepoints:
                res['meas_point'] = self.timepoints[dur]
            if self.time_temp and dur is not None:
                res['set_temp'] = self.polyline(dur, self.time_temp)
            if self.time_humid and dur is not None:
                res['set_humid'] = self.polyline(dur, self.time_humid)
            self.q_resp.put(res)

    def startprog(self, delay=31):
        """Create tickers for meas.*, power.*, telnet.* and binder.state
and add them to timer"""
        starttime = datetime.now() + timedelta(seconds=60+delay)
        starttime = starttime.replace(second=0, microsecond=0)
        self.stoptime = starttime + timedelta(seconds=self.progdur)
        self.starttime = starttime
        message = starttime.strftime('starting ESS program at %H:%M, duration')
        message += ' %d:%02d' % (self.progdur // 60, self.progdur % 60)
        logging.getLogger('ESSprogram').info(message)
        offset = (self.starttime - self.timer.basetime).total_seconds()
        # start and stop binder program
        self.timer.add_ticker('binder.state',
                              point_ticker(((0, self.progno),
                                            (self.progdur, None)), offset))
        if self.meas_ramps:
            self.timer.add_ticker('meas.ramp',
                                  point_ticker(self.meas_ramps, offset,
                                               'meas_point',
                                               self.timepoints))
        if self.meas_noises:
            self.timer.add_ticker('meas.noise',
                                  point_ticker(self.meas_noises, offset,
                                               'meas_point',
                                               self.timepoints))
        if self.meas_pulses:
            self.timer.add_ticker('meas.pulse',
                                  point_ticker(self.meas_pulses, offset,
                                               'meas_point',
                                               self.timepoints))
        if self.meas_freqs:
            self.timer.add_ticker('meas.freq',
                                  point_ticker(self.meas_freqs, offset,
                                               'meas_point',
                                               self.timepoints))
        if self.meas_ivs:
            self.timer.add_ticker('meas.iv',
                                  point_ticker(self.meas_ivs, offset,
                                               'meas_point',
                                               self.timepoints))
        if self.power_points:
            self.timer.add_ticker('power',
                                  point_ticker(self.power_points, offset))
        if self.logins:
            self.timer.add_ticker('telnet.login',
                                  point_ticker(self.logins, offset))
        if self.logouts:
            self.timer.add_ticker('telnet.logout',
                                  point_ticker(self.logouts, offset))
        if self.cmds:
            self.timer.add_ticker('telnet.cmds',
                                  point_ticker(self.cmds, offset))
        if self.flirs:
            self.timer.add_ticker('flir',
                                  point_ticker(self.flirs, offset))
        if self.test_points:
            self.timer.add_ticker('power.test',
                                  list_ticker(self.test_points, offset,
                                              'test_point',
                                              self.timepoints))
        if self.timerstop is not None:
            self.timer.add_ticker(
                'stop', one_tick(None, delay=offset+self.timerstop))
