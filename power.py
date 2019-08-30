"""
 ESS procedure
 control of power supply Rohde&Schwarz HMP4040 and TTi CPX400SP
"""

import logging
import re
import threading
from serial import Serial, SerialException

POWER_OPER = ('voltage', 'currLim', 'on', 'off')


class PowerSupply(threading.Thread):
    """Class for control of programable power supply
Developed for Rohde & Schwarz MHP4040 and for TTi CPX400SP."""
    re_cpx = re.compile(rb'.*CPX400')
    re_hmp = re.compile(rb'.*HMP4040')

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
                       bytesize=8, parity='N', stopbits=1, timeout=0.5)
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
        self.config(**kwargs)

    def run(self):
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                self.logger.info('Timer stopped, quitting PowerSupply.run()')
                return
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            if 'power' in flags:
                self.config(**flags['power'])
            if 'meas.iv' in flags and self.q_resp is not None:
                voltage, current = self.readVoltCurr()
                self.q_resp.put({'timestamp': timestamp,
                                 'ps_u': voltage,
                                 'ps_i': current})
        self.stop()

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
            self.ser.write(b'INST OUT%d\n' % ch)
            self.ser.write(b'OUTP:STATE %s\n' % bytes(state, 'ascii'))

    def _setVoltage_hmp(self, ch, value):
        self.logger.debug('Set voltage ch%d: %fV', ch, value)
        self.ser.write(b'INST OUT%d\n' % ch)
        self.ser.write(b'VOLT %f\n' % value)

    def _setCurrLim_hmp(self, ch, value):
        self.logger.debug('Set current limit ch%d: %fA', ch, value)
        self.ser.write(b'INST OUT%d\n' % ch)
        self.ser.write(b'CURR %f\n' % value)

    def _setVoltCurrLim_hmp(self, ch, voltage, currLim):
        self.logger.debug('Set voltage and current limit ch%d: %fV %fA',
                          ch, voltage, currLim)
        self.ser.write(b'INST OUT%d\n' % ch)
        self.ser.write(b'APPL %f, %f\n' % (voltage, currLim))

    def _readVoltCurr_hmp(self, chans=None):
        rchans = (self.uubch, ) if chans is None else chans
        res = {}
        for ch in rchans:
            self.ser.write(b'INST OUT%d\n' % ch)
            self.ser.write(b'MEAS:VOLT?\n')
            respv = self.ser.read(10)
            self.ser.write(b'MEAS:CURR?\n')
            respi = self.ser.read(10)
            try:
                m = re.match(rb'(-?[0-9]+(\.[0-9]*))', respv)
                voltage = float(m.groups()[0])
                m = re.match(rb'(-?[0-9]+(\.[0-9]*))', respi)
                current = float(m.groups()[0])
            except AttributeError:
                self.logger.error(
                    'Error reading HMP voltage/current at ch%d :' +
                    'respv "%s", respi "%s"', ch, repr(respv), repr(respi))
                return None
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
            self.ser.write(b'OP1 %d\n' % state)

    def _setVoltage_cpx(self, ch, value):
        self.logger.debug('Set voltage ch1: %fV', value)
        self.ser.write(b'V1 %f\n' % value)

    def _setCurrLim_cpx(self, ch, value):
        self.logger.debug('Set current limit ch: %fA', value)
        self.ser.write(b'I1 %f\n' % value)

    def _setVoltCurrLim_cpx(self, ch, voltage, currLim):
        self.logger.debug('Set voltage and current limit: %fV %fA',
                          voltage, currLim)
        self.ser.write(b'V1 %f\n' % voltage)
        self.ser.write(b'I1 %f\n' % currLim)

    def _readVoltCurr_cpx(self, ch=None):
        self.ser.write(b'V1O?\n')
        respv = self.ser.read(10)
        self.ser.write(b'I1O?\n')
        respi = self.ser.read(10)
        try:
            m = re.match(rb'(-?[0-9]+(\.[0-9]*))V', respv)
            voltage = float(m.groups()[0])
            m = re.match(rb'(-?[0-9]+(\.[0-9]*))A', respi)
            current = float(m.groups()[0])
        except AttributeError:
            self.logger.error(
                'Error reading CPX voltage/current: respv "%s", respi "%s"',
                repr(respv), repr(respi))
            return (None, None)
        self.logger.debug('Read voltage %.3fV, current %.3fA',
                          voltage, current)
        return (voltage, current)

    def stop(self):
        self.logger.info('Closing serial')
        try:
            self.ser.close()
        except Exception:
            pass
        self.stop = self._noaction

    def __del__(self):
        self.stop()

    def _noaction(self):
        pass
