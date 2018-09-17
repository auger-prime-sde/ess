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
    re_cpx = re.compile(r'.*CPX400')
    re_hmp = re.compile(r'.*HMP4040')

    def __init__(self, port, timer, **kwargs):
        """Constructor.
port - serial port to connect
kwargs - parameters for output voltage/current limit configuration
"""
        super(PowerSupply, self).__init__()
        self.timer = timer
        self.logger = logging.getLogger('PowerSup')
        s = None
        try:
            s = Serial(port, baudrate=9600, xonxoff=True,
                       bytesize=8, parity='N', stopbits=1, timeout=0.5)
            s.write('*IDN?\n')
            resp = s.read(100)
            self.logger.info('Connected, %s', resp)
        except SerialException:
            self.logger.exception('Init serial failed')
            if isinstance(s, Serial):
                self.logger.info('Closing serial %s', s.port)
                s.close()
            raise
        self.ser = s
        if PowerSupply.re_cpx.match(resp):
            setattr(PowerSupply, "output", PowerSupply._output_cpx)
            setattr(PowerSupply, "setVoltage", PowerSupply._setVoltage_cpx)
            setattr(PowerSupply, "setCurrLim", PowerSupply._setCurrLim_cpx)
            setattr(PowerSupply, "setVoltCurrLim",
                    PowerSupply._setVoltCurrLim_cpx)
            self.NCHAN = 1       # number of output channels
            self.uubch = 1       # the only channel in CPX400
        elif PowerSupply.re_hmp.match(resp):
            setattr(PowerSupply, "output", PowerSupply._output_hmp)
            setattr(PowerSupply, "setVoltage", PowerSupply._setVoltage_hmp)
            setattr(PowerSupply, "setCurrLim", PowerSupply._setCurrLim_hmp)
            setattr(PowerSupply, "setVoltCurrLim",
                    PowerSupply._setVoltCurrLim_hmp)
            self.NCHAN = 4       # number of output channels
            self.uubch = None    # undefined for HMP4040
        else:
            self.logger.error('Unknown power supply')
            raise ValueError
        self.config(**kwargs)

    def run(self):
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                self.logger.info('Timer stopped, quitting PowerSupply.run()')
                return
            # timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            if 'power' in flags:
                self.config(**flags['power'])

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
        args = {i: dict(map(None, POWER_OPER, kwargs.get('ch%d' % i, ())))
                for i in xrange(self.NCHAN+1)}
        # copy ch0 to uubch where uubch's value is None
        if self.uubch is not None:
            for key in POWER_OPER:
                if args[self.uubch][key] is None:
                    args[self.uubch][key] = args[0][key]
        # discard eventual uubch
        args.pop(0, None)

        # switch off
        chans = [i for i in args.keys() if args[i]['off']]
        self.output(chans, 0)
        # set voltage/current limit for all channels
        for i, d in args.iteritems():
            if d['voltage'] is not None:
                if d['currLim'] is not None:
                    self.setVoltCurrLim(i, d['voltage'], d['currLim'])
                else:
                    self.setVoltage(i, d['voltage'])
            elif d['currLim'] is not None:
                self.setCurrLim(i, d['currLim'])
        # switch on
        chans = [i for i in args.keys() if args[i]['on']]
        self.output(chans, 1)

    # HMP4040 methods
    def _output_hmp(self, chans, state):
        """Set channels in chans to state
chans - list of channels to switch
state - required state: 'ON' | 'OFF' | 0 | 1
"""
        if state in (0, 1):
            state = 'ON' if state else 'OFF'
        for ch in chans:
            self.logger.debug('Switch ch%d %s', ch, state)
            self.ser.write('INST OUT%d\n' % ch)
            self.ser.write('OUTP:STATE %s\n' % state)

    def _setVoltage_hmp(self, ch, value):
        self.logger.debug('Set voltage ch%d: %fV', ch, value)
        self.ser.write('INST OUT%d\n' % ch)
        self.ser.write('VOLT %f\n' % value)

    def _setCurrLim_hmp(self, ch, value):
        self.logger.debug('Set current limit ch%d: %fA', ch, value)
        self.ser.write('INST OUT%d\n' % ch)
        self.ser.write('CURR %f\n' % value)

    def _setVoltCurrLim_hmp(self, ch, voltage, currLim):
        self.logger.debug('Set voltage and current limit ch%d: %fV %fA',
                          ch, voltage, currLim)
        self.ser.write('INST OUT%d\n' % ch)
        self.ser.write('APPL %f, %f\n' % (voltage, currLim))

    # CPX400 methods
    def _output_cpx(self, chans, state):
        if 1 in chans:
            pstate = 'ON' if state else 'OFF'
            self.logger.debug('Switch ch1 %s', pstate)
            self.ser.write('OP1 %d\n' % state)

    def _setVoltage_cpx(self, ch, value):
        self.logger.debug('Set voltage ch1: %fV', value)
        self.ser.write('V1 %f\n' % value)

    def _setCurrLim_cpx(self, ch, value):
        self.logger.debug('Set current limit ch: %fA', value)
        self.ser.write('I1 %f\n' % value)

    def _setVoltCurrLim_cpx(self, ch, voltage, currLim):
        self.logger.debug('Set voltage and current limit ch: %fV %fA',
                          voltage, currLim)
        self.ser.write('V1 %f\n' % voltage)
        self.ser.write('I1 %f\n' % currLim)

    def __del__(self):
        self.logger.info('Closing serial')
        try:
            self.ser.close()
        except Exception:
            pass
