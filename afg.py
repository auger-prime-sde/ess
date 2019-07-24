"""
 ESS procedure
 control of AFG 3102C/3252C
 RPi trigger
"""

import logging
import math
import os
import errno
import signal
from subprocess import Popen
from binascii import unhexlify
from struct import pack

YSCALE = 0x3FFE    # data point value range <0, YSCALE>, including


class AFG(object):
    """Class for control of AFG over usbtmc"""

    # default values of parameters
    PARAM = {'functype': 'P',
             'gains': (1.0, None),  # gain for channels, off if None
             'offsets': (0.0, 0.0),    # offset for channels
             'usernum': 4,
             'hswidth': 0.625,  # us
             'Pvoltage': 1.6,   # pulse amplitude in V
             'freq': 1.0e6,     # Hz
             'Fvoltage': 0.5}    # sine amplitude in V
    SETCHANNEL = """\
output{ch:d}:state off
source{ch:d}:burst:mode triggered
source{ch:d}:burst:state ON
source{ch:d}:burst:tdelay 0
source{ch:d}:voltage:unit Vpp
output{ch:d}:impedance 50 Ohm
output{ch:d}:polarity {polarity:s}
source{ch:d}:voltage:level:immediate:{hilo:s} {zero:f}
"""
    SETFUNPULSE = """\
source{ch:d}:function user{usernum:d}
source{ch:d}:burst:ncycles 1
source{ch:d}:frequency {pulse_freq:f}MHz
source{ch:d}:phase 0 deg
"""
    SETFUNFREQ = """\
source{ch:d}:function sinusoid
source{ch:d}:phase 90 deg
"""

    def __init__(self, tmcid=1, zLoadUserfun=False, **kwargs):
        """Constructor.
tmcid - number of usbtmc device (default 1, i.e. /dev/usbtmc1)
zLoadUserfun - if True, load halfsine as a user function
kwargs - parameters:
  functype - type of signal P (5 half-sine pulses), F (sinusoid), default P
  gains - 2-tuple of voltage gain (float) or None (if the channel is off)
  offsets - 2-tuple of voltage offsets for channels (float, volt)
for functype P:
  usernum - user function number (default 4)
  hswidth - width of half sine in microseconds (default 0.625 us ~ 80kHz)
  Pvoltage - amplitude of sine in volt (default 1.6)
for functype F:
  freq - frequency of sinusiod in Hz (default 1e6)
  Fvoltage - amplitude of sinusiod in volt (default 0.5)
"""
        self.logger = logging.getLogger('AFG')
        device = '/dev/usbtmc%d' % tmcid
        self.DURATION = 22e-6  # duration of fun FREQ in seconds
        self.fd = None
        try:
            self.fd = os.open(device, os.O_RDWR)
            self.logger.debug('%s open', device)
            os.write(self.fd, b'*IDN?')
            resp = os.read(self.fd, 1000)
            self.logger.info('Connected, %s', resp)
        except OSError as e:
            if e.errno == errno.ENOENT:
                self.logger.error('Device %s does not exist (ENOENT)', device)
            elif e.errno == errno.EACCES:
                self.logger.error('Access to %s denied (EACCESS)', device)
            else:
                self.logger.error('Error opening %s - %s, %s',
                                  device, errno.errorcode[e.errno], e.args[1])
            if self.fd is not None:
                os.close(self.fd)
            raise
        # get parameters from kwargs with PARAM as default
        params = {key: kwargs.get(key, AFG.PARAM[key])
                  for key in AFG.PARAM}
        # initialize the AFG device
        self.send('trigger:sequence:source ext')
        self.param = {'functype': None, 'gains': (None, None)}
        self.setParams(**params)
        # takes about 15s
        if zLoadUserfun:
            self.writeUserfun(halfsine, params['usernum'],
                              5000, 5000/(20*math.pi))

    def stop(self):
        self.logger.info('Switching off channel and closing')
        for ch in (0, 1):
            if self.param['gains'][ch] is not None:
                self.send('output%d:state off' % (ch+1))
        os.close(self.fd)
        self.stop = self._noaction

    def __del__(self):
        self.stop()

    def _noaction(self):
        pass

    def send(self, line, lvl=logging.DEBUG):
        """Send line to AFG
lvl - optional logging level
"""
        line.rstrip()
        self.logger.log(lvl, 'Sending %s', line)
        os.write(self.fd, bytes(line, 'ascii'))

    def setParams(self, **d):
        """Set AFG parameters according to dictionary d.
Updates self.param and send them to AFG."""
        if 'functype' in d and d['functype'] != self.param['functype']:
            setFun = d['functype']
        else:
            setFun = None
        setChans = set()
        for ch in (0, 1):
            if 'gains' in d and d['gains'][ch] != self.param['gains'][ch]:
                if d['gains'][ch] is None:  # switch off ch before removing
                    self.switchOn(False, (ch, ))
                else:
                    setChans.add(ch)
            elif (self.param['gains'][ch] is not None and
                  'offsets' in d and
                  d['offsets'][ch] != self.param['offsets'][ch]):
                setChans.add(ch)
        self.param.update({key: d[key]
                           for key in AFG.PARAM if key in d})
        for ch in setChans:
            self._setChannel(ch)
        if setFun == 'P' or setChans and self.param['functype'] == 'P':
            self.logger.info('setting functype P, usernum %d, hswidth %fus',
                             self.param['usernum'], self.param['hswidth'])
            pulse_freq = 1./(20*self.param['hswidth'])  # us -> MHz
            chans = (0, 1) if setFun == 'P' else setChans
            for ch in chans:
                if self.param['gains'][ch] is None:
                    continue
                paramlines = AFG.SETFUNPULSE.format(
                    ch=ch+1, pulse_freq=pulse_freq,
                    usernum=self.param['usernum'])
                for line in paramlines.splitlines():
                    self.send(line)
        if setFun == 'F' or setChans and self.param['functype'] == 'F':
            self.logger.info('setting functype F')
            chans = (0, 1) if setFun == 'F' else setChans
            for ch in chans:
                if self.param['gains'][ch] is None:
                    continue
                for line in AFG.SETFUNFREQ.format(ch=ch+1).splitlines():
                    self.send(line)
        if setFun == 'P' or 'Pvoltage' in d and self.param['functype'] == 'P':
            voltage = self.param['Pvoltage']
            self.logger.info('setting Pvoltage %fV', voltage)
            for ch in (0, 1):
                self._setAmpli(ch, voltage)
        elif setFun == 'F' or (
                'Fvoltage' in d and self.param['functype'] == 'F'):
            self.logger.info('setting Fvoltage %fV', self.param['Fvoltage'])
            for ch in (0, 1):
                self._setAmpli(ch, 2*self.param['Fvoltage'])
        elif setChans:
            voltage = self.param['Pvoltage'] if self.param['functype'] == 'P' \
                      else 2 * self.param['Fvoltage']
            for ch in setChans:
                self._setAmpli(ch, voltage)
        if setFun == 'F' or 'freq' in d and self.param['functype'] == 'F':
            freq = self.param['freq']
            self.logger.info('setting freq %fHz', freq)
            ncycles = math.ceil(self.DURATION * freq)
            for ch in (0, 1):
                if self.param['gains'][ch] is None:
                    continue
                self.send("source%d:frequency %fHz" % (ch+1, freq))
                self.send("source%d:burst:ncycles %d" % (ch+1, ncycles))

    def switchOn(self, state=True, chans=(0, 1)):
        """Switch on/off outputs"""
        state = 'on' if state else 'off'
        self.logger.info('setting channels %s', state)
        for ch in chans:
            if self.param['gains'][ch] is not None:
                self.send("output%d:state %s" % (ch+1, state))

    def trigger(self):
        """Send trigger signal"""
        self.send("trigger")

    def writeUserfun(self, func, usernum, npoint=5000, scale=None):
        """Write function to AFG
func     - function [0:npoint/scale] -> [0:1]
usernum  - user function number
npoint   - number of function values
scale    - scaling of X (number of points corresponding to interva <0, 1>
           set to npoint if None
"""
        assert npoint > 0
        scale = float(npoint) if scale is None else float(scale)
        self.logger.info('Writing user function of %d points', npoint)
        self.send('data:define ememory,%d' % npoint)
        for i in range(npoint):
            value = int(YSCALE * func(i/scale) + 0.5)
            if value > YSCALE:
                value = YSCALE
            elif value < 0:
                value = 0
            self.send('data:data:value ememory,%d,%d' % (i+1, value),
                      logging.DEBUG - 1)
        self.send('data:lock user%d,off' % usernum)
        self.send('data:copy user%d,ememory' % usernum)
        self.send('data:lock user%d,on' % usernum)

    def _setChannel(self, ch):
        """Initilize channel ch according to gain and offset"""
        gain = self.param['gains'][ch]
        if gain is None:
            return
        zero = self.param['offsets'][ch]
        self.logger.info('setting channel %d, gain %f, offset %f',
                         ch+1, gain, zero)
        hilo = 'high' if gain > 0 else 'low'  # N.B. inverted against _setAmpli
        polarity = 'normal' if gain > 0.0 else 'inverted'
        for line in AFG.SETCHANNEL.format(ch=ch+1, zero=zero, hilo=hilo,
                                          polarity=polarity).splitlines():
            self.send(line)

    def _setAmpli(self, ch, voltage):
        """Set voltage amplitude (p-p) on channel ch,
according to gain and offset
voltage - peak to peak amplitude
ch - AFG channel 0 or 1
"""
        gain = self.param['gains'][ch]
        if gain is None:
            return
        hilo = 'low' if gain > 0 else 'high'
        self.send('source%d:voltage:level:immediate:%s %fV' %
                  (ch+1, hilo, self.param['offsets'][ch] - gain * voltage))


def halfsine(x):
    """ 1 - sin(x) on <0,pi> + 4*pi*n; 1 otherwise"""
    xx = x % (4*math.pi)
    val = 1 - math.sin(xx) if xx < math.pi else 1.0
    return val


def writeTFW(func, filename, npoint=5000, scale=None):
    """ Write function values in TFW format
func     - function [0:npoint/Tscale] -> [0:1]
filename - file to write TFW (.tfw appended if not present)
npoint   - number of function values
scale    - scaling of X (number of points corresponding to interva <0, 1>
           set to npoint if None
"""
    header = 'TEKAFG3000' + '\0'*6 + unhexlify('0131f0c2')
    nprev = 412   # number of point in preview
    nzeros = 0x200 - len(header) - 8 - nprev
    assert nzeros == 72

    assert npoint > 0
    scale = float(npoint) if scale is None else float(scale)
    pscale = scale * nprev / npoint  # scale for preview
    values = [int(YSCALE * func(i/scale) + 0.5) for i in range(npoint)]
    values = [YSCALE if val > YSCALE else 0 if val < 0 else val
              for val in values]
    preview = [int(0xFF * func(ii/pscale) + 0.5) for ii in range(nprev)]
    preview = [0xFF if val > 0xFF else 0 if val < 0 else val
               for val in preview]

    if not filename.lower().endswith('.tfw'):
        filename += '.tfw'
    with open(filename, 'wb') as fout:
        fout.write(header)
        fout.write(pack('>LL', npoint, 1))
        fout.write(''.join([chr(x) for x in preview]))
        fout.write('\0' * nzeros)
        fout.write(''.join([pack('>H', x) for x in values]))


class RPiTrigger(object):
    """Class for trigger on Raspberry Pi"""
    PULSE_BIN = '/home/suma/pulse'  # binary to manage GPIO

    def __init__(self):
        self.proc = Popen([self.PULSE_BIN])

    def stop(self):
        self.proc.terminate()
        self.proc.poll()
        self.stop = self._noaction

    def __del__(self):
        self.stop()

    def _noaction(self):
        pass

    def trigger(self):
        self.proc.send_signal(signal.SIGUSR1)


if __name__ == '__main__':
    writeTFW(halfsine, 'halfsine', 5000, 5000/(2*math.pi * 10))
