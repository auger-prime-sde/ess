#
# ESS procedure - communication

from datetime import datetime, timedelta
import re
import threading
import logging
import json

# ESS stuff
from timer import one_tick, point_ticker, list_ticker
from binder import Binder
from threadid import syscall, SYS_gettid


class Chamber(threading.Thread):
    """Thread managing Climate chamber"""
    stopstate = 'manual'
    nonprog_states = ('manual', 'idle')

    def __init__(self, binder, timer, q_resp):
        """Constructor.
binder - instance of Binder derivative
timer - instance of Timer
q_resp - queue to send response"""
        super(Chamber, self).__init__()
        assert isinstance(binder, Binder)
        self.binder = binder
        self.timer = timer
        self.q_resp = q_resp
        self.logger = logging.getLogger('chamber')
        self.binder.get_state()

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
            if 'binder.start_prog' in flags:
                try:
                    progno = int(flags['binder.start_prog'])
                    self.logger.info('Starting program %d', progno)
                    self.binder.start_prog(progno)
                except Exception as e:
                    self.logger.error(
                        'Error starting chamber program. %s', e)
            if 'binder.stop_prog' in flags:
                manual = flags.get['binder.stop_prog']
                self.binder.stop_prog(manual)
            if 'meas.thp' in flags:
                self.logger.debug('Chamber temperature & humidity measurement')
                temperature = self.binder.get_temp()
                humid = self.binder.get_humid()
                self.logger.debug('Done. t = %.2fdeg.C, h = %.2f%%',
                                  temperature, humid)
                res = {'timestamp': timestamp,
                       'meas_thp': True,
                       'chamber_temp': temperature,
                       'chamber_humid': humid}
                self.q_resp.put(res)
            if 'binder.load_prog' in flags:
                progno, prog = (flags['binder.load_prog']['progno'],
                                flags['binder.load_prog']['prog'])
                self.logger.info('Loading program %d', progno)
                self.binder.load_prog(progno, prog)

    def stop(self, state=None):
        """Stop a program running in chamber
to to manual mode if state == 'manual', else idle"""
        if state is None:
            state = self.stopstate
        if self.binder is not None and isinstance(self.binder, Binder):
            self.binder.stop_prog(state == 'manual')


class ChamberProg(object):
    """Representation of climate chamber program"""
    def __init__(self, temperature, humidity=None, anticond=False, title=b''):
        """Constructor
temperature - initial temperature [float]
humidity - initial humidity [float or None]
         - if None, no control of humidity
anticond - apply anticond contact by default"""
        self.temperature = temperature
        self.humidity = humidity
        self.anticond = anticond
        self.title = title
        self.segments = []
        self.cycles = []  # (<first segment>, #repeat, <last segment>)

    def _startcycle(self, currentseg, repeat):
        if self.cycles:
            assert self.cycles[-1][2] is not None, "Nested cycles"
        repeat = int(repeat)
        assert repeat >= 0, "Negative cycle repeat number"
        self.cycles.append([currentseg, repeat, None])

    def _endcycle(self, currentseg):
        assert self.cycles and self.cycles[-1][2] is None, \
            "End cycle while not in cycle"
        self.cycles[-1][2] = currentseg

    def add_segment(self, **segment):
        """Add segment to program
segment - dictionary
   duration - [int]. Segment length (seconds, positive)
   temperature - [float or None/not present]. If None, use the previous value
   humidity - as temperature. Ignored if initial humidity None
   anticond - apply anticondensation, if None use previous value
   startcycle - [int or None/not present]. Non-negative number of repetions
   endcycle - [True or None/not present]. Close the cycle
   label - [str or None/not present]. Label of the segment.
         - evaluated as label.format(i=<cycle iteration, 1..repeat>)
"""
        nseg = {}
        assert isinstance(segment['duration'], int),\
            "Missing/wrong format of duration"
        assert segment['duration'] > 0, "Duration not positive"
        nseg['duration'] = segment['duration']
        temp = segment.get('temperature', None)
        if temp is not None:
            temp = float(temp)  # make sure temp is float
        nseg['temperature'] = temp
        if self.humidity is not None:
            humid = segment.get('humidity', None)
            assert humid is None or isinstance(humid, float),\
                "Wrong format of humidity"
            nseg['humidity'] = humid
        else:
            nseg['humidity'] = None
        nseg['anticond'] = segment.get('anticond', None)
        startcycle = segment.get('startcycle', None)
        if startcycle is not None:
            self._startcycle(nseg, startcycle)
        if segment.get('endcycle', False):
            self._endcycle(nseg)
        self.segments.append(nseg)

    def compact(self):
        """Compact segments - TBD"""
        pass

    def __str__(self):
        return "Chamber program dump TBD"


class ESSprogram(threading.Thread):
    """Build BinderProg and ticker from JSON description"""

    class _points(object):
        """Container for measurement poits et al."""
        def __init__(self):
            self.mrs = {}   # measurement ADC ramp: {time: flags}
            self.mns = {}   # measurement noise: {time: flags}
            self.mps = {}   # measurement pulses, {time: flags}
            self.mfs = {}   # measurement freqs, {time: flags}
            self.tps = []   # test points: [time]
            self.pcs = []   # PowerControl check [time]
            self.pps = {}   # power operations: {time: kwargs}
            # flags for lis/los/cms: None or list of UUBnums
            self.lis = {}   # logins {time: flags}
            self.los = {}   # logouts {time: flags}
            self.cms = {}   # telnet cmds {time: flags}
            self.dls = {}   # telnet downloads {time: flags}
            self.fls = {}   # flir operations {time: flags}
            self.els = {}   # evaluator operations {time: flags}
            self.lto = {}   # log_timeout: {time: log_timeout value}
            self.ts = {}    # timers {time: flags}
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
            for dname in ('mrs', 'mns', 'mps', 'mfs', 'pps', 'ts',
                          'lis', 'los', 'cms', 'dls', 'fls', 'els', 'lto'):
                self.__dict__[dname].update(
                    {t + self.t: val
                     for t, val in otherpoints.__dict__[dname].items()})
            self.tps.extend([t + self.t for t in otherpoints.tps])
            self.pcs.extend([t + self.t for t in otherpoints.pcs])

        def keys(self):
            """Return all time points"""
            keys = self.tps
            for dname in ('mrs', 'mns', 'mps', 'mfs', 'pps', 'ts',
                          'lis', 'los', 'cms', 'dls', 'fls', 'els', 'lto'):
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
        temperature = jso.get('temperature', None)
        if temperature is not None:
            humidity = jso.get('humidity', None)
            anticond = jso.get('anticond', False)
            title = bytes(jso.get('title', ''), 'utf-8')
            self.prog = ChamberProg(temperature, humidity, anticond, title)
            self.prog.stop_manual = jso.get('stop_manual', None)
        else:
            self.prog = None
        gp = ESSprogram._points()  # global points
        cp = None                  # points in cycle
        numrepeat = None           # number to repeat
        ap = gp                    # actual points
        self.timerstop = None
        for segment in jso['program']:
            if 'stop' in segment:  # stop timer after a delay
                assert self.timerstop is None, "Double stop timer"
                assert cp is None, "Stop timer while in cycle"
                self.timerstop = gp.t + int(segment['stop'])
                continue
            else:
                assert self.timerstop is None, "Segment after stop"
            binderseg = {}
            if 'startcycle' in segment:
                assert cp is None, "Nested cycle"
                numrepeat = segment['startcycle']
                assert isinstance(numrepeat, int) and numrepeat >= 0
                binderseg['startcycle'] = numrepeat
                cp = ESSprogram._points()
                ap = cp

            binderseg['duration'] = dur = segment["duration"]
            # temperature, humidity & operc
            for key in ('temperature', 'humidity', 'anticond'):
                binderseg[key] = segment.get(key, None)
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
                    kwargs = {}
                    if pp.get('pccheck', None):
                        kwargs['check'] = True
                        check_time = self._macro(pp['pccheck'])
                        ap.pcs.append(ptime + check_time)
                        log_timeout = check_time + 2
                        if ptime not in ap.lto or log_timeout > ap.lto[ptime]:
                            ap.lto[ptime] = log_timeout
                    # kwargs for PowerSupply.config()
                    # e.g. {'ch1': (12.0, None, True, False)}
                    for chan in pp:
                        if re.match(r'^ch\d$', chan) is None:
                            continue
                        args = self._macro(pp[chan])
                        if not all([v is None for v in args]):
                            kwargs[chan] = args
                    # PowerControl
                    # pcon, pcoff parameter: list of UUBs, True or None
                    # rz_tout parameter: wait time [s] for readZone, float
                    for key in ('pcon', 'pcoff', 'rz_tout',
                                'pczero', 'pccalib'):
                        if key in pp:
                            kwargs[key] = self._macro(pp[key])
                    if 'volt_ramp' in pp:
                        volt_ramp = self._macro(pp['volt_ramp'])
                        kwargs['volt_ramp'] = {
                            key: self._macro(volt_ramp[key])
                            for key in ('volt_start', 'volt_end', 'volt_step',
                                        'time_step', 'start')}
                        vstep = abs(kwargs['volt_ramp']['volt_step'])
                        if kwargs['volt_ramp']['volt_end'] < \
                           kwargs['volt_ramp']['volt_start']:
                            vstep = -vstep
                        kwargs['volt_ramp']['volt_step'] = vstep
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
                    if "dloads" in tp:
                        filelist = [str(self._macro(fn))
                                    for fn in self._macro(tp["dloads"])]
                        if "dloads.uubs" in tp:
                            uubnums = self._macro(tp["dloads.uubs"])
                        else:
                            uubnums = None
                        ap.dls[ttime] = {"filelist": filelist,
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
            # Evaluator
            if 'eval' in segment:
                eval = self._macro(segment["eval"])
                for ep in eval:
                    ep = self._macro(ep)
                    offset = self._macro(ep["offset"])
                    etime = ap.t + offset
                    if offset < 0:
                        etime += dur
                    flags = {key: self._macro(ep[key])
                             for key in ('checkISN', 'orderUUB', 'removeUUB',
                                         'message')
                             if key in ep}
                    ap.els[etime] = flags
            # Timer
            if 'timer' in segment:
                timers = self._macro(segment["timer"])
                for t in timers:
                    t = self._macro(t)
                    offset = self._macro(t["offset"])
                    ttime = ap.t + offset
                    if offset < 0:
                        ttime += dur
                    recs = filter(self._macro, self._macro(t['recs']))
                    ap.ts[ttime] = {'recs': recs}
            ap.t += dur
            if segment.get('endcycle', False):
                assert cp is not None, "End cycle while not in cycle"
                binderseg['endcycle'] = True
                for i in range(numrepeat):
                    gp.update(cp)
                    gp.t += cp.t
                cp, numrepeat = None, None
                ap = gp
            if self.prog:
                self.prog.add_segment(**binderseg)
        assert cp is None, "Unfinished cycle"
        self.progdur = gp.t
        self.time_temp = gp.time_temp
        self.time_humid = gp.time_humid
        # append the last segments
        self.meas_ramps = [(mptime, gp.mrs[mptime])
                           for mptime in sorted(gp.mrs)]
        self.meas_noises = [(mptime, gp.mns[mptime])
                            for mptime in sorted(gp.mns)]
        self.meas_pulses = [(mptime, gp.mps[mptime])
                            for mptime in sorted(gp.mps)]
        self.meas_freqs = [(mptime, gp.mfs[mptime])
                           for mptime in sorted(gp.mfs)]
        self.power_points = [(ptime, gp.pps[ptime])
                             for ptime in sorted(gp.pps)]
        self.test_points = sorted(gp.tps)
        self.pcc_points = sorted(gp.pcs)
        self.logins = [(ttime, gp.lis[ttime]) for ttime in sorted(gp.lis)]
        self.logouts = [(ttime, gp.los[ttime]) for ttime in sorted(gp.los)]
        self.cmds = [(ttime, gp.cms[ttime]) for ttime in sorted(gp.cms)]
        self.dloads = [(ttime, gp.dls[ttime]) for ttime in sorted(gp.dls)]
        self.flirs = [(ftime, gp.fls[ftime]) for ftime in sorted(gp.fls)]
        self.evals = [(etime, gp.els[etime]) for etime in sorted(gp.els)]
        self.timers = [(ttime, gp.ts[ttime]) for ttime in sorted(gp.ts)]
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
            self.timer.add_immediate(
                'binder.load_prog',
                {'prog': self.prog, 'progno': self.progno})
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
            # modify db_noise -> db_noisestat if count in flags[meas.noise]
            if 'meas.noise' in flags and 'count' in flags['meas.noise'] and \
               'db_noise' in res:
                res['db_noisestat'] = res.pop('db_noise')
            if dur in self.timepoints:
                res['meas_point'] = self.timepoints[dur]
            if self.time_temp and dur is not None:
                res['set_temp'] = self.polyline(dur, self.time_temp)
            if self.time_humid and dur is not None:
                res['set_humid'] = self.polyline(dur, self.time_humid)
            self.q_resp.put(res)

    def startprog(self, delay=31):
        """Create tickers for meas.*, power.*, telnet.* and binder.*
and add them to timer"""
        starttime = datetime.now() + timedelta(seconds=60+delay)
        starttime = starttime.replace(second=0, microsecond=0)
        self.stoptime = starttime + timedelta(seconds=self.progdur)
        self.starttime = starttime
        message = starttime.strftime('starting ESS program at %H:%M, duration')
        message += ' %d:%02d' % (self.progdur // 60, self.progdur % 60)
        logging.getLogger('ESSprogram').info(message)
        offset = (self.starttime - self.timer.basetime).total_seconds()
        if self.prog is not None:
            self.timer.add_ticker(
                'binder.start_prog',
                one_tick(None, delay=offset, detail=self.progno))
            self.timer.add_ticker(
                'binder.stop_prog',
                one_tick(None, delay=offset+self.progdur,
                         detail=self.prog.stop_manual))
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
        if self.dloads:
            self.timer.add_ticker('telnet.dloads',
                                  point_ticker(self.dloads, offset))
        if self.flirs:
            self.timer.add_ticker('flir',
                                  point_ticker(self.flirs, offset))
        if self.evals:
            self.timer.add_ticker('eval',
                                  point_ticker(self.evals, offset))
        if self.timers:
            self.timer.add_ticker('timer',
                                  point_ticker(self.timers, offset))
        if self.test_points:
            self.timer.add_ticker('power.test',
                                  list_ticker(self.test_points, offset,
                                              'test_point',
                                              self.timepoints))
        if self.pcc_points:
            self.timer.add_ticker('power.pccheck',
                                  list_ticker(self.pcc_points, offset))
        if self.timerstop is not None:
            self.timer.add_ticker(
                'stop', one_tick(None, delay=offset+self.timerstop))
