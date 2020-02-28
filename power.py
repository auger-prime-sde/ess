"""
 ESS procedure
 control of power supply Rohde&Schwarz HMP4040 and TTi CPX400SP
"""

import logging
import re
import threading
from serial import Serial, SerialException
from time import sleep
from datetime import datetime, timedelta

from threadid import syscall, SYS_gettid
from BME import readSerRE, SerialReadTimeout
POWER_OPER = ('voltage', 'currLim', 'on', 'off')


class PowerSupply(threading.Thread):
    """Class for control of programable power supply
Developed for Rohde & Schwarz MHP4040 and for TTi CPX400SP."""
    re_cpx = re.compile(rb'.*CPX400')
    re_hmp = re.compile(rb'.*HMP4040')
    RE_FLOAT = rb'(-?[0-9]+(\.[0-9]*)?)'
    re_hmp_val = re.compile(RE_FLOAT)
    re_cpx_volt = re.compile(RE_FLOAT + rb'V')
    re_cpx_curr = re.compile(RE_FLOAT + rb'A')
    EPS = 1e-3  # epsilon to mitigate rounding errors in nstep calculation

    def __init__(self, port, timer=None, q_resp=None, **kwargs):
        """Constructor.
port - serial port to connect
kwargs - parameters for output voltage/current limit configuration
"""
        super(PowerSupply, self).__init__()
        self.timer = timer
        self.q_resp = q_resp
        logger = logging.getLogger('PowerSup')
        s = None
        try:
            s = Serial(port, baudrate=9600, xonxoff=True,
                       bytesize=8, parity='N', stopbits=1, timeout=2.0)
            s.write(b'*IDN?\n')
            resp = s.read(100)
            logger.info('Connected, %s', resp)
        except SerialException:
            logger.exception('Init serial failed')
            if isinstance(s, Serial):
                logger.info('Closing serial %s', s.port)
                s.close()
            raise
        self.ser = s
        if PowerSupply.re_cpx.match(resp):
            self.logger = logging.getLogger('PowerSup_cpx')
            setattr(self, "output",
                    PowerSupply._output_cpx.__get__(self, PowerSupply))
            setattr(self, "setVoltage",
                    PowerSupply._setVoltage_cpx.__get__(self, PowerSupply))
            setattr(self, "setCurrLim",
                    PowerSupply._setCurrLim_cpx.__get__(self, PowerSupply))
            setattr(self, "setVoltCurrLim",
                    PowerSupply._setVoltCurrLim_cpx.__get__(self, PowerSupply))
            setattr(self, "readVoltCurr",
                    PowerSupply._readVoltCurr_cpx.__get__(self, PowerSupply))
            self.NCHAN = 1       # number of output channels
            self.uubch = 1       # the only channel in CPX400
        elif PowerSupply.re_hmp.match(resp):
            self.logger = logging.getLogger('PowerSup_hmp')
            setattr(self, "output",
                    PowerSupply._output_hmp.__get__(self, PowerSupply))
            setattr(self, "setVoltage",
                    PowerSupply._setVoltage_hmp.__get__(self, PowerSupply))
            setattr(self, "setCurrLim",
                    PowerSupply._setCurrLim_hmp.__get__(self, PowerSupply))
            setattr(self, "setVoltCurrLim",
                    PowerSupply._setVoltCurrLim_hmp.__get__(self, PowerSupply))
            setattr(self, "readVoltCurr",
                    PowerSupply._readVoltCurr_hmp.__get__(self, PowerSupply))
            self.NCHAN = 4       # number of output channels
            self.uubch = None    # undefined for HMP4040
        else:
            logger.error('Unknown power supply')
            raise ValueError
        self._lock = threading.Lock()
        self.vramps = []  # list of (ts_end, thread)
        self.config(**kwargs)

    def run(self):
        tid = syscall(SYS_gettid)
        self.logger.debug('run start, name %s, tid %d', self.name, tid)
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                break
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags

            # join old volt_ramp threads
            if self.vramps:
                vramps = []  # new list for unfinished ramps
                for ts, thr in self.vramps:
                    if ts is None or ts < timestamp:
                        thr.join(0.001)
                        if thr.is_alive():
                            self.logger.warning('Join %s timeouted', thr.name)
                            vramps.append((None, thr))
                    else:
                        vramps.append((ts, thr))
                self.vramps = vramps

            if 'power' in flags:
                # interpret uubch, ch%d
                self.config(**flags['power'])

            if 'meas.sc' in flags and self.q_resp is not None:
                voltage, current = self.readVoltCurr()
                self.q_resp.put({'timestamp': timestamp,
                                 'meas_sc': True,
                                 'ps_u': voltage,
                                 'ps_i': current})

            if 'power' in flags and 'volt_ramp' in flags['power']:
                live_vr = [thr for ts, thr in self.vramps
                           if ts is not None]
                if live_vr:
                    self.logger.error('Still running voltage ramps: %s',
                                      ', '.join([thr.name for thr in live_vr]))
                volt_ramp = self._voltRamp_validate(
                    flags['power']['volt_ramp'], ts_start=timestamp)
                thr = threading.Thread(target=self.voltageRamp,
                                       args=(volt_ramp, ))
                self.vramps.append((volt_ramp['ts_end'], thr))
                thr.start()

        self.logger.info('run finished')

    def config(self, **kwargs):
        """Configuration of output paramters
ch<n>: (voltage, curr. limit, on, off) - set on/off, voltage, curr.limit
   if a parameter is not None
"""
        uubch = kwargs.get('uubch', None)
        if uubch is not None:
            if not (isinstance(uubch, int) and 1 <= uubch <= self.NCHAN):
                self.logger.error('Wrong uubch %s', repr(uubch))
            else:
                self.logger.debug('Set UUB ch = %d', uubch)
                self.uubch = uubch
        args = {}
        for i in range(self.NCHAN+1):
            args[i] = {k: None for k in POWER_OPER}
            argtuple = kwargs.get('ch%d' % i, None)
            if argtuple is not None:
                args[i].update(dict(list(zip(POWER_OPER, argtuple))))
        # copy ch0 to uubch where uubch's value is None
        if self.uubch is not None:
            for key in POWER_OPER:
                if args[self.uubch][key] is None:
                    args[self.uubch][key] = args[0][key]
        # discard eventual uubch
        args.pop(0, None)

        # switch off
        chans = [i for i in args if args[i]['off']]
        self.output(chans, 0)
        # set voltage/current limit for all channels
        for i, d in list(args.items()):
            if d['voltage'] is not None:
                if d['currLim'] is not None:
                    self.setVoltCurrLim(i, d['voltage'], d['currLim'])
                else:
                    self.setVoltage(i, d['voltage'])
            elif d['currLim'] is not None:
                self.setCurrLim(i, d['currLim'])
        # switch on
        chans = [i for i in args if args[i]['on']]
        self.output(chans, 1)

    def _voltRamp_validate(self, volt_ramp, ts_start):
        """Validate voltage ramp parameters
params as in voltageRamp
adjust volt_step
add ts_start, ts_end, tdelta, duration, nstep to volt_ramp"""
        if ts_start is None:
            ts_start = datetime.now()
        vstep = abs(volt_ramp['volt_step'])
        nstep = int((abs(volt_ramp['volt_end'] - volt_ramp['volt_start'])
                     + self.EPS) / vstep)
        if volt_ramp['volt_end'] < volt_ramp['volt_start']:
            vstep = -vstep
        volt_ramp['nstep'] = nstep
        volt_ramp['volt_step'] = vstep
        volt_ramp['tdelta'] = timedelta(seconds=volt_ramp['time_step'])
        volt_ramp['ts_start'] = ts_start
        volt_ramp['ts_end'] = ts_start + volt_ramp['tdelta'] * nstep
        volt_ramp['duration'] = volt_ramp['time_step'] * nstep
        return volt_ramp

    def voltageRamp(self, volt_ramp, ts_start=None):
        """Perform voltage ramp
volt_ramp - dict with keys: volt_start, volt_end, volt_step, time_step
ts_start - start time (now() if None)"""
        tid = syscall(SYS_gettid)
        self.logger.debug('voltageRamp: name %s, tid %d', self.name, tid)
        assert self.uubch is not None, "Channel for voltage ramp not provided"
        ch = self.uubch
        if any([key not in volt_ramp
                for key in ('nstep', 'ts_start', 'ts_end', 'tdelta')]):
            self._voltRamp_validate(volt_ramp, ts_start)
        MSG = 'voltage ramp [{volt_start:.1f}:{volt_step:.1f}:' + \
              '{volt_end:.1f}] V, duration {duration:.1f}s'
        self.logger.info(MSG.format(**volt_ramp))
        volt = volt_ramp['volt_start']
        timestamp = volt_ramp['ts_start']
        self.setVoltage(ch, volt)
        for _ in range(volt_ramp['nstep']):
            volt += volt_ramp['volt_step']
            timestamp += volt_ramp['tdelta']
            delta = timestamp - datetime.now()
            sec = delta.seconds + 0.000001 * delta.microseconds
            if sec > 0.0:
                sleep(sec)
            self.setVoltage(ch, volt)

    # HMP4040 methods
    def _output_hmp(self, chans, state):
        """Set channels in chans to state
chans - list of channels to switch
state - required state: 'ON' | 'OFF' | 0 | 1 | False | True
"""
        if state in (0, 1, False, True):
            state = 'ON' if state else 'OFF'
        assert state in ('ON', 'OFF')
        for ch in chans:
            self.logger.debug('Switch ch%d %s', ch, state)
            with self._lock:
                self.ser.write(b'INST OUT%d\n' % ch)
                self.ser.write(b'OUTP:STATE %s\n' % bytes(state, 'ascii'))

    def _setVoltage_hmp(self, ch, value):
        self.logger.debug('Set voltage ch%d: %fV', ch, value)
        with self._lock:
            self.ser.write(b'INST OUT%d\n' % ch)
            self.ser.write(b'VOLT %f\n' % value)

    def _setCurrLim_hmp(self, ch, value):
        self.logger.debug('Set current limit ch%d: %fA', ch, value)
        with self._lock:
            self.ser.write(b'INST OUT%d\n' % ch)
            self.ser.write(b'CURR %f\n' % value)

    def _setVoltCurrLim_hmp(self, ch, voltage, currLim):
        self.logger.debug('Set voltage and current limit ch%d: %fV %fA',
                          ch, voltage, currLim)
        with self._lock:
            self.ser.write(b'INST OUT%d\n' % ch)
            self.ser.write(b'APPL %f, %f\n' % (voltage, currLim))

    def _readVoltCurr_hmp(self, chans=None):
        rchans = (self.uubch, ) if chans is None else chans
        res = {}
        for ch in rchans:
            self.logger.info('Reading voltage & current for ch%d', ch)
            respv = respi = ''
            try:
                with self._lock:
                    self.ser.write(b'INST OUT%d\n' % ch)
                    self.ser.write(b'MEAS:VOLT?\n')
                    respv = readSerRE(self.ser, PowerSupply.re_hmp_val,
                                      timeout=0.1, logger=self.logger)
                    self.ser.write(b'MEAS:CURR?\n')
                    respi = readSerRE(self.ser, PowerSupply.re_hmp_val,
                                      timeout=0.1, logger=self.logger)
            except (SerialReadTimeout, AttributeError):
                self.logger.error(
                    'Error reading HMP voltage/current at ch%d :' +
                    'respv "%s", respi "%s"', ch, repr(respv), repr(respi))
                return None
            m = re.match(PowerSupply.re_hmp_val, respv)
            voltage = float(m.groups()[0])
            m = re.match(PowerSupply.re_hmp_val, respi)
            current = float(m.groups()[0])
            self.logger.debug('Read voltage %.3fV, current %.3fA',
                              voltage, current)
            res[ch] = (voltage, current)
        return res[self.uubch] if chans is None else res

    # CPX400 methods
    def _output_cpx(self, chans, state):
        """Set channels in chans to state
chans - list of channels to switch, no action if 1 not in chans
state - required state: 'ON' | 'OFF' | 0 | 1 | False | True
"""
        assert state in (0, 1, False, True, 'ON', 'OFF')
        if 1 in chans:
            state = 1 if state in (1, True, 'ON') else 0
            pstate = 'ON' if state else 'OFF'
            self.logger.debug('Switch ch1 %s', pstate)
            with self._lock:
                self.ser.write(b'OP1 %d\n' % state)

    def _setVoltage_cpx(self, ch, value):
        self.logger.debug('Set voltage ch1: %fV', value)
        with self._lock:
            self.ser.write(b'V1 %f\n' % value)

    def _setCurrLim_cpx(self, ch, value):
        self.logger.debug('Set current limit ch: %fA', value)
        with self._lock:
            self.ser.write(b'I1 %f\n' % value)

    def _setVoltCurrLim_cpx(self, ch, voltage, currLim):
        self.logger.debug('Set voltage and current limit: %fV %fA',
                          voltage, currLim)
        with self._lock:
            self.ser.write(b'V1 %f\n' % voltage)
            self.ser.write(b'I1 %f\n' % currLim)

    def _readVoltCurr_cpx(self, ch=None):
        self.logger.info('Reading voltage & current')
        respv = respi = ''
        try:
            with self._lock:
                self.ser.write(b'V1O?\n')
                respv = readSerRE(self.ser, PowerSupply.re_cpx_volt,
                                  timeout=0.1, logger=self.logger)
                self.ser.write(b'I1O?\n')
                respi = readSerRE(self.ser, PowerSupply.re_cpx_curr,
                                  timeout=0.1, logger=self.logger)
        except (SerialReadTimeout, AttributeError):
            self.logger.error(
                'Error reading CPX voltage/current: respv "%s", respi "%s"',
                repr(respv), repr(respi))
            return (None, None)
        m = re.match(PowerSupply.re_cpx_volt, respv)
        voltage = float(m.groups()[0])
        m = re.match(PowerSupply.re_cpx_curr, respi)
        current = float(m.groups()[0])
        self.logger.debug('Read voltage %.3fV, current %.3fA',
                          voltage, current)
        return (voltage, current)

    def stop(self):
        try:
            for ts, thr in self.vramps:
                thr.join()
            self.ser.close()
        except Exception:
            pass
        self.stop = self._noaction

    def __del__(self):
        self.stop()

    def _noaction(self):
        pass
