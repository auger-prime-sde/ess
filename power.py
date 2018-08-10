"""
 ESS procedure
 control of power supply Rohde&Schwarz HMP4040
"""

import logging
import threading
from serial import Serial, SerialException

POWER_OPER = ('voltage', 'currLim', 'on', 'off')

class PowerSupply(threading.Thread):
    """Class for control of programable power supply
Developed for Rohde & Schwarz MHP4040."""
    def __init__(self, port, timer, **kwargs):
        """Constructor.
port - serial port to connect
kwargs - parameters for output voltage/current limit configuration
"""
        super(PowerSupply, self).__init__()
        self.timer = timer
        self.logger = logging.getLogger('PowerSup')
        self.NCHAN = 4       # number of output channels
        s = None
        try:
            s = Serial(port, baudrate=9600,
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
        self.uubch = None
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
        chans = [i for i in args.keys() if args[i]['off'] == True]
        self.output(chans, 'OFF')
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
        chans = [i for i in args.keys() if args[i]['on'] == True]
        self.output(chans, 'ON')

    def output(self, chans, state):
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
            
    def setVoltage(self, ch, value):
        self.logger.debug('Set voltage ch%d: %fV', ch, value)
        self.ser.write('INST OUT%d\n' % ch)
        self.ser.write('VOLT %f\n' % value)

    def setCurrLim(self, ch, value):
        self.logger.debug('Set current limit ch%d: %fA', ch, value)
        self.ser.write('INST OUT%d\n' % ch)
        self.ser.write('CURR %f\n' % value)

    def setVoltCurrLim(self, ch, voltage, currLim):
        self.logger.debug('Set voltage and current limit ch%d: %fV %fA',
                          ch, voltage, currLim)
        self.ser.write('INST OUT%d\n' % ch)
        self.ser.write('APPL %f, %f\n' % (voltage, currLim))

    def __del__(self):
        self.logger.info('Closing serial')
        try:
            self.ser.close()
        except Exception:
            pass
