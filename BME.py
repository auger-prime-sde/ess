#
# ESS procedure
# communication with BME280 on arduino
#

import re
import math
import threading
import logging
from time import sleep
from datetime import datetime, timedelta
from serial import Serial

import numpy as np

from dataproc import item2label
from threadid import syscall, SYS_gettid


class SerialReadTimeout(AssertionError):
    pass


def readSerRE(ser, r, timeout=2, logger=None):
    """Try to read regexp 're' from serial with timeout
r - compiled regexp
timeout - timeout in s after which SerialReadTimeout exception is raised
return response from serial or raise SerialReadTimeout exception"""
    TIME_STEP = 0.01   # timestep between successive read trials
    resp = ser.read(ser.inWaiting())
    if r.match(resp):
        if logger is not None:
            logger.debug("serial %s read %s" % (ser.port, repr(resp)))
        return resp
    tend = datetime.now() + timedelta(seconds=timeout)
    while datetime.now() < tend:
        if ser.inWaiting() > 0:
            resp += ser.read(ser.inWaiting())
            if r.match(resp):
                if logger is not None:
                    logger.debug("serial %s read %s" % (ser.port, repr(resp)))
                return resp
        sleep(TIME_STEP)
    if logger is not None:
        logger.debug("serial %s timed out, partial read %s" % (
            ser.port, repr(resp)))
    raise SerialReadTimeout


# mapping DS18B20 hw addresses to paper labels
DS_HW = {'28-0308977930c1': 0,
         '28-030797794c45': 1,
         '28-03079779bfb6': 2,
         '28-030797793e0f': 4,
         '28-03079779d0fd': 5,
         '28-030897795424': 6,
         '28-03089779846d': 7,
         '28-0308977967fb': 8,
         '28-030797797db4': 9}


class BME(threading.Thread):
    """Thread managing arduino reading BME280"""
    re_bmeinit = re.compile(rb'.*BME.*\r\n', re.DOTALL)
    re_set = re.compile(rb'.*OK\r\n', re.DOTALL)
    RE_RTC = rb'.*(?P<dt>20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})'
    RE_BME = (rb' +(?P<temp1>-?\d+(\.\d*)?).*' +
              rb' +(?P<humid1>\d+(\.\d*)?).*' +
              rb' +(?P<press1>\d+(\.\d*)?).*' +
              rb' +(?P<temp2>-?\d+(\.\d*)?).*' +
              rb' +(?P<humid2>\d+(\.\d*)?).*' +
              rb' +(?P<press2>\d+(\.\d*)?)')
    re_dsline = re.compile(rb'.*DS\[(\d+)\]: (28-[a-fA-F0-9]{12})')
    RE_DSTEMP = rb' +(?P<dstemp%d>-?\d+(\.\d*)?)'
    FLAG_RTC = 1
    FLAG_SYNC = 2

    def __init__(self, port, timer=None, q_resp=None, flags=0):
        """Constructor.
port - serial port to connect
timer - instance of timer
q_resp - queue to send response
flags - 1: use RTC -or- 2: sync Arduino time
"""
        super(BME, self).__init__(name='Thread-BME')
        self.timer = timer
        self.q_resp = q_resp
        # check that we are connected to BME
        self.logger = logging.getLogger('bme')
        s = None               # avoid NameError on isinstance(s, Serial) check
        self.dsmap = {}
        try:
            s = Serial(port, baudrate=115200)
            self.logger.info('Opening serial %s', repr(s))
            readSerRE(s, BME.re_bmeinit, timeout=3, logger=self.logger)
            # set with/without RTC
            s.write(b'r' if flags & BME.FLAG_RTC else b'R')
            readSerRE(s, BME.re_set, timeout=1, logger=self.logger)
            if flags & BME.FLAG_SYNC:
                # initialize time
                self.logger.info('BME time sync')
                ts = (datetime.now() + timedelta(seconds=1)).strftime(
                    "t %Y-%m-%dT%H:%M:%S\r")
                s.write(bytes(ts, 'ascii'))
                readSerRE(s, BME.re_set, timeout=3, logger=self.logger)
                self.logger.info('synced to %s', ts)
            # detect DS18B20
            s.write(b'd')
            respds = readSerRE(s, BME.re_set, timeout=1, logger=self.logger)
        except Exception:
            self.logger.exception("Init serial with BME failed")
            if isinstance(s, Serial):
                self.logger.info('Closing serial %s', s.port)
                s.close()
            raise SerialReadTimeout
        self.ser = s
        # prepare RE for measurement results
        for line in respds.split(b'\n'):
            m = BME.re_dsline.match(line)
            if m is not None:
                ind, hw = m.groups()
                hw = hw.decode('ascii').lower()
                if hw in DS_HW:
                    self.dsmap[int(ind)] = DS_HW[hw]
                else:
                    self.logger.warning('unknown DS %s', hw)
        self.nds = len(self.dsmap)
        self.logger.info(
            'Detected ' + ', '.join(['DS%d' % self.dsmap[i]
                                     for i in range(self.nds)]))
        re_meas = BME.RE_RTC if flags & BME.FLAG_RTC else rb'\s*'
        re_meas += BME.RE_BME
        re_meas += b''.join([BME.RE_DSTEMP % i for i in range(self.nds)])
        self.re_meas = re.compile(re_meas + rb'\r\n', re.DOTALL)

    def measure(self, timestamp=None):
        """Perform measurement, return dict with results"""
        self.logger.debug('BME read')
        self.ser.write(b'm')
        resp = readSerRE(self.ser, self.re_meas, logger=self.logger)
        self.logger.debug('BME read finished')

        d = self.re_meas.match(resp).groupdict()
        if 'dt' in d:
            bmetime = d.pop('dt').decode('ascii')
            bmetimestamp = datetime.strptime(bmetime, '%Y-%m-%dT%H:%M:%S')
            self.logger.debug('BME vs event time diff: %f s',
                              (bmetimestamp - timestamp).total_seconds())
        res = {'bme_' + k: float(d[k])
               for k in ('temp1', 'temp2', 'humid1', 'humid2',
                         'press1', 'press2')}
        for i in range(self.nds):
            res['ds%d_temp' % self.dsmap[i]] = float(d['dstemp%d' % i])
        return res

    def dslist(self):
        """Return list of detected DS18B20 (as integers in label 'DS%d')"""
        return sorted(self.dsmap.values())

    def stop(self):
        try:
            self.ser.close()
        except Exception:
            pass
        self.stop = self._noaction

    def __del__(self):
        self.stop()

    def _noaction(self):
        pass

    def run(self):
        if self.timer is None or self.q_resp is None:
            self.logger.error('timer or q_resp instance not provided, exiting')
            return
        tid = syscall(SYS_gettid)
        self.logger.debug('run start, name %s, tid %d',
                          threading.current_thread().name, tid)
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                self.logger.info('Timer stopped')
                break
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            if 'meas.thp' in flags:
                try:
                    res = self.measure(timestamp)
                except SerialReadTimeout:
                    self.logger.warning('BME read timeout')
                else:
                    res['timestamp'] = timestamp
                    res['meas_tmp'] = True
                    self.q_resp.put(res)
        self.logger.info('Run finished')


class TrigDelay(object):
    """Interface to arduino managing trigger delay"""
    re_init = re.compile(rb'.*TrigDelay (?P<version>\d+)\r\n', re.DOTALL)
    re_ok = re.compile(rb'.*OK\r\n', re.DOTALL)
    re_getdelay = re.compile(rb'.*trigdelay .*: (?P<delay>\d+)\r\n', re.DOTALL)

    def __init__(self, port, predefined=None):
        """Constructor.
port - serial port to connect
predefined - dict functype: delay with predefined values """
        self.logger = logging.getLogger('TrigDelay')
        s = None               # avoid NameError on isinstance(s, Serial) check
        try:
            s = Serial(port, baudrate=115200)
            self.logger.info('Opening serial %s', repr(s))
            sleep(1.0)  # ad hoc constant to avoid timeout
            # s.write(b'?\r')
            resp = readSerRE(s, TrigDelay.re_init, timeout=1,
                             logger=self.logger)
            self.version = TrigDelay.re_init.match(resp).groupdict()['version']
        except Exception:
            self.logger.exception("Init serial with TrigDelay failed")
            if isinstance(s, Serial):
                self.logger.info('Closing serial %s', s.port)
                s.close()
            raise SerialReadTimeout
        self.ser = s
        self.predefined = predefined if predefined is not None else {}

    def __del__(self):
        self.stop()

    def stop(self):
        if self.ser is not None:
            self.ser.close()
            self.ser = None

    @property
    def delay(self):
        """Get delay from Arduino"""
        self.logger.info('getting delay')
        self.ser.write(b'q\r')
        resp = readSerRE(self.ser, TrigDelay.re_getdelay,
                         timeout=1, logger=self.logger)
        m = TrigDelay.re_getdelay.match(resp)
        return int(m.groupdict()['delay'])

    @delay.setter
    def delay(self, delay):
        """Set delay in <250ns> units"""
        ndelay = self.predefined.get(delay, delay)
        self.logger.info('setting delay %d * 3/16us', ndelay)
        self.ser.write(b'd %d\r' % ndelay)
        readSerRE(self.ser, TrigDelay.re_ok,
                  timeout=1, logger=self.logger)
        self.logger.debug('delay set')

    def trigger(self):
        """Send a trigger pulse"""
        self.ser.write(b't\r')
        readSerRE(self.ser, TrigDelay.re_ok,
                  timeout=1, logger=self.logger)
        self.logger.debug('trigger sent')


class PowerControl(threading.Thread):
    """Class managing power control module and splitter mode"""
    SPLITMODE_DEFAULT = 1
    re_init = re.compile(
        rb'.*PowerControl dev:(?P<device>\d) (?P<version>[-0-9]+)\r\n',
        re.DOTALL)
    re_set = re.compile(rb'.*OK', re.DOTALL)
    # ten floats separated by whitespaces + OK
    re_readcurr = re.compile(rb'.*?' + (rb'(-?\d+\.?\d*)\s+' * 10) +
                             rb'OK', re.DOTALL)
    # ten 0/1 symbols + OK
    re_readrelay = re.compile(rb'.*?([01]{10})\s*OK', re.DOTALL)
    # integer + OK
    re_atime = re.compile(rb'.*?(\d+)\s*OK', re.DOTALL)
    # (<pin>[+-]<final zone>:<mtime> )*
    re_zones = re.compile(rb'.*?(([0-9][+-][0-7]:\d+\s)*)\s*OK', re.DOTALL)
    re_zone = re.compile(rb'(?P<port>[0-9])(?P<dir>[+-])(?P<zone>[0-7]):' +
                         rb'(?P<atics>\d+)')
    RZ_TOUT = 30.  # [s] default timeout for rz_tout
    TICK = 0.0004  # [s] Arduino time tick
    NCHANS = 10   # number of channel
    NZONES = 3  # number of current zones; recompile Arduino fw if modified
    ZONEOVER = 7  # zone for overcurrent
    CURLIMS = (50.0, 250.0, 750.0)  # current limits in mA; recompile dtto
    ZONEFMT = '{ts:%Y-%m-%dT%H:%M:%S.%f} {atics:12d} ' + \
              '{uubnum:04d} {dir:1s} {zone:d}\n'
    USBCOMX = 0.5  # moment of Arduino action between start and end timestamp
    TCALIB = 60  # decay time for calibration weitgths

    def __init__(self, port, ctx=None, splitmode=None):
        """Constructor
port - serial port to connect
ctx - context object, used keys: timer, q_resp, datadir, basetime, uubnums

timer - instance of timer
q_resp - queue to send response
uubnums - list of UUBnums in order of connections.  None if port skipped"""
        self.logger = logging.getLogger('PowerControl')
        if ctx is None:
            self.timer, self.q_resp, self.fp = None, None, None
            self.uubnums = {}
            self.basetime = datetime.now().replace(microsecond=0)
        else:
            self.timer, self.q_resp = ctx.timer, ctx.q_resp
            assert len(ctx.uubnums) <= 10
            self.uubnums = {uubnum: port
                            for port, uubnum in enumerate(ctx.uubnums)
                            if uubnum is not None}
            luubnums = ' '.join(['%1d:%04d' % (port, uubnum)
                                 for port, uubnum in enumerate(ctx.uubnums)
                                 if uubnum is not None])
            self.port2uubnum = {port: uubnum
                                for uubnum, port in self.uubnums.items()}
            self.zones = {uubnum: 0 for uubnum in ctx.uubnums
                          if uubnum is not None}
            self.curlims = {uubnum: PowerControl.CURLIMS
                            for uubnum in ctx.uubnums if uubnum is not None}
            self.basetime = ctx.basetime
            fn = ctx.datadir + ctx.basetime.strftime('zones-%Y%m%d.log')
            self.fp = open(fn, 'a')
            self.fp.write("""\
# Current zone transition
# date %s
# port/UUBs: %s
# columns: time | atics | UUB | direction <+/-> | final zone
""" % (ctx.basetime.strftime('%Y-%m-%d'), luubnums))
        s = None               # avoid NameError on isinstance(s, Serial) check
        try:
            s = Serial(port, baudrate=115200)
            self.logger.info('Opening serial %s', repr(s))
            s.write(b'?\r')
            sleep(0.5)  # ad hoc constant to avoid timeout
            resp = readSerRE(s, PowerControl.re_init, timeout=1,
                             logger=self.logger)
            self.version = PowerControl.re_init.match(
                resp).groupdict()['version']
        except Exception:
            self.logger.exception("Init serial with PowerControl failed")
            if isinstance(s, Serial):
                self.logger.info('Closing serial %s', s.port)
                s.close()
            raise SerialReadTimeout
        self.ser = s
        self._lock = threading.Lock()
        super(PowerControl, self).__init__(name='Thread-PowerCtrl')
        self.uubnums2del = []
        self.splitterMode = splitmode
        self.atimestamp = None  # time of the last zeroTime
        self.tick = PowerControl.TICK  # default value
        self.curzones = []
        self.bootvolt = self.boottime = self.pendingLimits = self.chk_ts = None
        self.rz_tout = PowerControl.RZ_TOUT
        self.rz_thread = None
        self.zeroTime()

    def _removeUUB(self, uubnum):
        port = self.uubnums.pop(uubnum)
        del self.port2uubnum[port]
        del self.zones[uubnum]
        del self.curlims[uubnum]

    def setCurrLimits(self, limits, applynow=False):
        """Set current limits in Arduino
limits - list of (uubnum, limit_1, .. limit_NZONES)
       - if uubnum is None, set for all ports"""
        for limit in limits:
            assert len(limit) == PowerControl.NZONES + 1
            assert limit[0] is None or limit[0] in self.uubnums
            assert all([isinstance(curr, float) for curr in limit[1:]])
        self.pendingLimits = {limit[0]: tuple(limit[1:]) for limit in limits}
        if applynow:
            self._applyCurrLimits()

    def _applyCurrLimits(self):
        """Apply pending current limits to Arduino"""
        # thread safe poping + setting None
        pendingLimits = self.__dict__.pop('pendingLimits')
        self.__dict__.setdefault('pendingLimits')
        if pendingLimits is None:
            return
        for uubnum, currents in pendingLimits.items():
            port = '*' if uubnum is None else "%d" % self.uubnums[uubnum]
            pcurrents = ' '.join(['%d' % int(10*c + 0.5) for c in currents])
            self.logger.info('Setting current limits %c %s',
                             port, pcurrents)
            with self._lock:
                self.ser.write(bytes('l %c %s\r' % (port, pcurrents), 'ascii'))
                readSerRE(self.ser, PowerControl.re_set, logger=self.logger)
            if uubnum is None:
                for uubnum in self.uubnums:
                    self.curlims[uubnum] = currents
            else:
                self.curlims[uubnum] = currents
        self._readZones()  # discard old current zone transitions
        self.curzones = []

    def _get_splitterMode(self):
        """Return current setting of splitmode"""
        return self._splitmode

    def _set_splitterMode(self, mode=None):
        """Set splitter mode (0: attenuated, 1: frequency, 3: amplified)"""
        if mode is None:
            mode = PowerControl.SPLITMODE_DEFAULT
        assert mode in (0, 1, 3)  # allowed values
        self.logger.info('setting splitter mode %d', mode)
        with self._lock:
            self.ser.write(b'm %d\r' % mode)
            readSerRE(self.ser, PowerControl.re_set, timeout=1,
                      logger=self.logger)
        self.logger.debug('splitter mode set')
        self._splitmode = mode

    splitterMode = property(_get_splitterMode, _set_splitterMode)

    def splitterOn(self, state=None):
        """Switch splitter on/off"""
        if state is None:
            return
        self.logger.info('switching splitter %s', 'on' if state else 'off')
        cmd = b'1\r' if state else b'0\r'
        with self._lock:
            self.ser.write(cmd)
            readSerRE(self.ser, PowerControl.re_set, timeout=1,
                      logger=self.logger)
        self.logger.debug('splitter switched')

    def switch(self, state, uubs=None):
        """Switch on/off relays
state - True to switch ON, False to OFF
uubs - list of uubnums to switch or True to switch all
     - if None, switch only ports in self.uubnums"""
        if uubs is True:
            chans = (1 << self.NCHANS) - 1  # all chans
        elif uubs is None:
            chans = sum([1 << port for port in self.uubnums.values()])
        else:
            chans = sum([1 << self.uubnums[uubnum] for uubnum in uubs])
        cmd = 'n' if state else 'f'
        self.logger.info('switch: %c uubs=%s chans=%o', cmd, repr(uubs), chans)
        with self._lock:
            self.ser.write(bytes('%c %o\r' % (cmd, chans), 'ascii'))
            readSerRE(self.ser, PowerControl.re_set, logger=self.logger)

    def switchRaw(self, state, chans):
        """Switch on/off relays
state - True to switch ON, False to OFF
chans - bitmask of ports to switch"""
        assert 0 < chans < 2**self.NCHANS
        cmd = 'n' if state else 'f'
        self.logger.info('switchRaw: %c %o', cmd, chans)
        with self._lock:
            self.ser.write(bytes('%c %o\r' % (cmd, chans), 'ascii'))
            readSerRE(self.ser, PowerControl.re_set, logger=self.logger)

    def relays(self):
        """Read status of relays
return tuple of two list: (uubsOn, uubsOff)"""
        with self._lock:
            self.ser.write(b'd\r')
            resp = readSerRE(self.ser, PowerControl.re_readrelay,
                             logger=self.logger)
        states = PowerControl.re_readrelay.match(resp).groups()[0]
        uubsOn = [uubnum for uubnum, port in self.uubnums.items()
                  if states[port] == ord('1')]
        uubsOff = [uubnum for uubnum, port in self.uubnums.items()
                   if states[port] == ord('0')]
        return uubsOn, uubsOff

    def zeroTime(self):
        """Zero PowerControl internal time"""
        self.logger.info('Zero Arduino time')
        with self._lock:
            ts1 = datetime.now()
            self.ser.write(b't\r')
            readSerRE(self.ser, PowerControl.re_set, logger=self.logger)
            ts2 = datetime.now()
        self.atimestamp = ts1 + self.USBCOMX*(ts2 - ts1)
        self.calibtime = self.atimestamp
        dt = (self.atimestamp - self.basetime).total_seconds()
        self.XtX = np.array([[1.0, 0.0], [0.0, 0.0]], dtype='float64')
        self.XtY = np.array([[dt], [0.0]], dtype='float64')
        self.logger.debug('ts1 = %s, ts2 = %s, atimestamp = %s',
                          ts1.strftime("%M:%S.%f"), ts2.strftime("%M:%S.%f"),
                          self.atimestamp.strftime("%M:%S.%f"))

    def calibrateTime(self):
        """Read Arduino tick time and recalibrate"""
        assert self.atimestamp is not None, "zeroTime not called yet"
        with self._lock:
            ts1 = datetime.now()
            self.ser.write(b'c\r')
            resp = readSerRE(self.ser, PowerControl.re_atime,
                             logger=self.logger)
            ts2 = datetime.now()
        atime = ts1 + self.USBCOMX*(ts2 - ts1)
        atics = int(PowerControl.re_atime.match(resp).groups()[0])
        self.logger.debug(
            'Arduino time calibration ' +
            'ts1 = %s, ts2 = %s, atime = %s, tics = %d',
            ts1.strftime("%M:%S.%f"), ts2.strftime("%M:%S.%f"),
            atime.strftime("%M:%S.%f"), atics)
        dt = (atime - self.basetime).total_seconds()
        w = math.exp(math.atan(
            (self.calibtime - atime).total_seconds()/self.TCALIB))
        self.XtX = w*self.XtX + np.array([[1.0, atics], [atics, atics*atics]])
        self.XtY = w*self.XtY + np.array([[dt], [dt*atics]])
        res = np.linalg.lstsq(self.XtX, self.XtY)
        if res[2] < 2:
            self.logger.warning('Singularity in calibration, abandoned')
            return
        # store results, locking against atics2ts/zeroTime TBD
        self.atimestamp = self.basetime + timedelta(seconds=res[0][0, 0])
        self.tick = res[0][1, 0]
        self.calibtime = atime
        self.logger.info('new atimestamp = %s, tick = %7.3f us',
                         self.atimestamp.strftime("%M:%S.%f"),
                         1e6*self.tick)

    def atics2ts(self, atics):
        """Convert PowerControl internal time to timestamp"""
        return self.atimestamp + timedelta(seconds=self.tick * atics)

    def _readCurrents(self):
        """Read currents [mA]. Return as tuple of ten floats"""
        self.logger.info('reading currents')
        with self._lock:
            self.ser.write(b'r\r')
            resp = readSerRE(self.ser, PowerControl.re_readcurr,
                             logger=self.logger)
        return [float(s)
                for s in PowerControl.re_readcurr.match(resp).groups()]

    def _readZones(self):
        """Read zone transition and log them.
Return list of dict with keys: ts, uubnum, dir, zone"""
        self.logger.info('reading current zone transitions')
        with self._lock:
            self.ser.write(b'z\r')
            resp = readSerRE(self.ser, PowerControl.re_zones,
                             logger=self.logger)
        recs = []
        for rec in PowerControl.re_zones.match(resp).groups()[0].split(b' '):
            if not rec:
                continue
            d = PowerControl.re_zone.match(rec).groupdict()
            for key in ('port', 'zone', 'atics'):
                d[key] = int(d[key])
            d['dir'] = d['dir'].decode('ascii')
            try:
                d['uubnum'] = self.port2uubnum[d['port']]
            except KeyError:
                self.logger.warning('Transition in unassigned port: %s', rec)
                continue
            d['ts'] = self.atics2ts(d['atics'])
            if self.fp is not None:
                self.fp.write(PowerControl.ZONEFMT.format(**d))
            if d['zone'] == PowerControl.ZONEOVER:
                self.logger.error(
                    'OverCurrent on UUB %04d at %s', d['uubnum'],
                    d['ts'].strftime('%Y-%m-%d %H:%M:%S.%f'))
            recs.append(d)
        if recs:
            self.fp.flush()
        return recs

    def _voltres(self, uubnum):
        """Determine voltage when UUB either switched on or off
return voltage set when transition occurs
May raise IndexError if appropriate record does not exist"""
        bv = self.bootvolt  # shortcut
        if bv['start']:  # last 0->1 transition before first 1->N-1 trans
            self.logger.debug(   # ###
                'vres start %s',
                repr([rec for rec in self.curzones
                      if rec['uubnum'] == uubnum and rec['dir'] == '+']))
            ztime = [rec['ts'] for rec in self.curzones
                     if rec['uubnum'] == uubnum and rec['dir'] == '+' and
                     rec['zone'] == PowerControl.NZONES-1][0]
            atime = [rec['ts'] for rec in self.curzones
                     if rec['uubnum'] == uubnum and rec['dir'] == '+' and
                     rec['zone'] == 1 and rec['ts'] < ztime][-1]
        else:  # the first 1->0 transition after the last N-1->1 trans
            self.logger.debug(   # ###
                'vres stop %s',
                repr([rec for rec in self.curzones
                      if rec['uubnum'] == uubnum and rec['dir'] == '-']))
            ztime = [rec['ts'] for rec in self.curzones
                     if rec['uubnum'] == uubnum and rec['dir'] == '-' and
                     rec['zone'] == PowerControl.NZONES-2][-1]
            atime = [rec['ts'] for rec in self.curzones
                     if rec['uubnum'] == uubnum and rec['dir'] == '-' and
                     rec['zone'] == 0 and rec['ts'] > ztime][0]
        istep = int((atime - self.chk_ts).total_seconds() /
                    bv['time_step'])
        self.logger.debug('bootvolt = %s, istep = %d', repr(bv), istep)
        volt = bv['volt_start'] + istep * bv['volt_step']
        voltmin = min(bv['volt_start'], bv['volt_end'])
        if volt < voltmin:
            self.logger.warning(
                'ramp voltage for UUB %04d less than minimal voltage',
                uubnum)
            raise IndexError
        voltmax = max(bv['volt_start'], bv['volt_end'])
        if volt > voltmax:
            self.logger.warning(
                'ramp voltage for UUB %04d bigger than maximal voltage',
                uubnum)
            raise IndexError
        return volt

    def _boottime(self, uubnum):
        """Calculate time for boot (between 0->1 and 1->2 transition)
May raise IndexError if appropriate record does not exist"""
        # the first 0->1 transition
        a1 = [rec['ts'] for rec in self.curzones
              if rec['uubnum'] == uubnum and rec['dir'] == '+' and
              rec['zone'] == 1][0]
        a2 = [rec['ts'] for rec in self.curzones
              if rec['uubnum'] == uubnum and rec['dir'] == '+' and
              rec['zone'] == PowerControl.NZONES-1][-1]
        return (a2 - a1).total_seconds()

    def readZone(self):
        """Function running in a separate thread to read current zone
 transitions periodically"""
        tid = syscall(SYS_gettid)
        self.logger.debug('readZone: name %s, tid %d',
                          threading.current_thread().name, tid)
        tout = self.rz_tout
        while tout:
            self.curzones.extend(self._readZones())
            sleep(tout)
            tout = self.rz_tout
        self.logger.debug('readZone finished')

    def run(self):
        if self.timer is None or self.q_resp is None:
            self.logger.error('timer or q_resp instance not provided, exiting')
            return
        tid = syscall(SYS_gettid)
        self.logger.debug('run start, name %s, tid %d',
                          threading.current_thread().name, tid)
        self.rz_thread = threading.Thread(
            target=self.readZone, name='Thread-readZone')
        self.rz_thread.start()
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                break
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            while self.uubnums2del:
                self._removeUUB(self.uubnums2del.pop())

            if 'meas.sc' in flags:
                currents = self._readCurrents()
                res = {item2label(typ='itot', uubnum=uubnum): currents[port]
                       for uubnum, port in self.uubnums.items()}
                res['timestamp'] = timestamp
                res['meas_sc'] = True
                self.q_resp.put(res)

            if 'power' in flags:
                if 'rz_tout' in flags['power']:
                    tout = flags['power']['rz_tout']
                    self.rz_tout = tout if tout is not None else self.RZ_TOUT
                if flags['power'].get('pczero', False):
                    self.zeroTime()
                if flags['power'].get('pccalib', False):
                    self.calibrateTime()
                # valid uubs for pcon/pcoff: <list>, True, None
                if 'pcoff' in flags['power']:
                    self.switch(False, flags['power']['pcoff'])
                if 'pcon' in flags['power']:
                    uubs = flags['power']['pcon']
                    del self.curzones[:]
                    self.switch(True, uubs)
                    if 'check' in flags['power']:
                        if self.boottime or self.bootvolt:
                            self.logger.warning('pcon: already under check')
                            self.bootvolt = None
                        self.boottime = True
                        self.chk_ts = timestamp
                if 'volt_ramp' in flags['power']:
                    if self.boottime or self.bootvolt:
                        self.logger.warning('voltramp: already under check')
                    del self.curzones[:]
                    bv = flags['power']['volt_ramp']
                    bv['up'] = bv['volt_start'] < bv['volt_end']
                    self.bootvolt = bv
                    self.boottime = self.bootvolt['start']
                    self.chk_ts = timestamp

            if 'power.pccheck' in flags:
                if not self.bootvolt and not self.boottime:
                    self.logger.error('not after pcon or volt_ramp')
                    continue
                res = {'timestamp': self.chk_ts}
                if self.bootvolt:
                    direction = 'up' if self.bootvolt['up'] else 'down'
                    state = 'on' if self.bootvolt['start'] else 'off'
                    res['volt_ramp'] = (direction, state)
                    typ = 'voltramp' + direction + state
                    for uubnum in self.uubnums:
                        label = item2label(typ=typ, uubnum=uubnum)
                        try:
                            res[label] = self._voltres(uubnum)
                        except IndexError:
                            self.logger.warning('voltage for %s not available',
                                                label)
                    self.bootvolt = None
                if self.boottime:
                    for uubnum in self.uubnums:
                        label = item2label(typ='boottime', uubnum=uubnum)
                        try:
                            res[label] = self._boottime(uubnum)
                        except IndexError:
                            self.logger.warning(
                                'boottime for %04d not available', uubnum)
                    self.boottime = None
                self.q_resp.put(res)
        self.logger.info('run finished')

    def stop(self):
        try:
            if self.fp is not None:
                self.fp.close()
            if self.rz_thread:
                self.rz_tout = None
                self.rz_thread.join()
            self.ser.close()
        except Exception:
            pass
        self.stop = self._noaction

    def __del__(self):
        self.stop()

    def _noaction(self):
        pass
