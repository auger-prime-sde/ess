"""
 ESS procedure
 control of AFG 3102C
"""

import logging
import math
import os
import errno
from binascii import unhexlify
from struct import pack

YSCALE = 0x3FFE    # data point value range <0, YSCALE>, including


class AFG(object):
    """Class for control of AFG over usbtmc"""

    # default values of parameters
    PARAM = {'functype': 'P',
             'polarity': 'inverted',
             'ch2': False,
             'usernum': 4,
             'hswidth': 0.625,  # us
             'Pvoltage': 1.6,   # pulse amplitude in V
             'freq': 1.0e6,     # Hz
             'Fvoltage': 0.5}    # sine amplitude in V
    SETINIT = """\
output1:state off
source1:burst:mode triggered
source1:burst:state ON
source1:burst:tdelay 0
source1:voltage:level:immediate:low 0
source1:voltage:unit Vpp
output1:impedance 50 Ohm
output1:polarity {polarity:s}
trigger:sequence:source ext
output2:state off
source2:function:shape square
source2:frequency 2kHz
source2:voltage:level:immediate:low -2.5V
source2:voltage:level:immediate:high 2.5V
output2:impedance 50 Ohm
"""
    SETFUNPULSE = """\
source1:function user{usernum:d}
source1:burst:ncycles 1
source1:frequency {pulse_freq:f}MHz
"""
    SETFUNFREQ = """\
source1:function sinusoid
source1:phase 90 deg
"""

    def __init__(self, tmcid=1, zLoadUserfun=False, **kwargs):
        """Constructor.
tmcid - number of usbtmc device (default 1, i.e. /dev/usbtmc1)
zLoadUserfun - if True, load halfsine as a user function
kwargs - parameters:
  functype - type of signal P (5 half-sine pulses), F (sinusoid), default P
  polarity - normal | inverted (default inverted)
  ch2 - True | False - if ch2 in splitter on (default False)
for functype P:
  usernum - user function number (default 4)
  hswidth - width of half sine in microseconds (default 0.625 us ~ 80kHz)
  Pvoltage - amplitude of sine in Volt (default 1.6)
for functype F:
  freq - frequency of sinusiod in Hz (default 1e6)
  Fvoltage - amplitude of sinusiod in Volt (default 0.8)
"""
        self.logger = logging.getLogger('AFG')
        device = '/dev/usbtmc%d' % tmcid
        self.DURATION = 22e-6  # duration of fun FREQ in seconds
        self.fd = None
        try:
            self.fd = os.open(device, os.O_RDWR)
            self.logger.debug('%s open', device)
            os.write(self.fd, '*IDN?')
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
        # takes about 15s
        if zLoadUserfun:
            self.writeUserfun(halfsine, self.param['usernum'],
                              5000, 5000/(20*math.pi))
        # get parameters from kwargs with PARAM as default
        params = {key: kwargs.get(key, AFG.PARAM[key])
                  for key in AFG.PARAM}
        # initialize the AFG device
        for line in AFG.SETINIT.format(**params).splitlines():
            self.send(line)
        self.param = {'functype': None, 'ch2': None}
        self.setParams(**params)

    def __del__(self):
        self.logger.info('Switching off channels and closing')
        self.send('output1:state off')
        self.send('output2:state off')
        os.close(self.fd)

    def send(self, line, lvl=logging.DEBUG):
        """Send line to AFG
lvl - optional logging level
"""
        line.rstrip()
        self.logger.log(lvl, 'Sending %s', line)
        os.write(self.fd, line)

    def setParams(self, **d):
        """Set AFG parameters according to dictionary d.
Updates self.param and send them to AFG."""
        if 'functype' in d and d['functype'] != self.param['functype']:
            setFun = d['functype']
        else:
            setFun = None
        setCh2 = 'ch2' in d and d['ch2'] != self.param['ch2']
        self.param.update({key: d[key]
                           for key in AFG.PARAM.keys() if key in d})
        if setFun == 'P':
            self.logger.info('setting functype P, usernum %d, hswidth %fus',
                             self.param['usernum'], self.param['hswidth'])
            pulse_freq = 1./(20*self.param['hswidth'])  # us -> MHz
            paramlines = AFG.SETFUNPULSE.format(pulse_freq=pulse_freq,
                                                usernum=self.param['usernum'])
            for line in paramlines.splitlines():
                self.send(line)
        if setFun == 'F':
            self.logger.info('setting functype F')
            for line in AFG.SETFUNFREQ.splitlines():
                self.send(line)
        if setFun == 'P' or 'Pvoltage' in d and self.param['functype'] == 'P':
            self.logger.info('setting Pvoltage %fV', self.param['Pvoltage'])
            self.send("source1:voltage:level:immediate:high %fV" %
                      self.param['Pvoltage'])
        if setFun == 'F' or 'Fvoltage' in d and self.param['functype'] == 'F':
            self.logger.info('setting Fvoltage %fV', self.param['Fvoltage'])
            self.send("source1:voltage:level:immediate:high %fV" %
                      (2*self.param['Fvoltage']))
        if setFun == 'F' or 'freq' in d and self.param['functype'] == 'F':
            self.logger.info('setting freq %fHz', self.param['freq'])
            ncycles = math.ceil(self.DURATION * self.param['freq'])
            self.send("source1:frequency %fHz" % self.param['freq'])
            self.send("source1:burst:ncycles %d" % ncycles)
        if setCh2:
            state = 'on' if self.param['ch2'] else 'off'
            self.logger.info('setting ch2 %s', state)
            self.send("output2:state " + state)

    def switchOn(self, state=True):
        """Switch on signal on ch1"""
        state = 'on' if state else 'off'
        self.logger.info('setting ch1 %s', state)
        self.send("output1:state " + state)

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
        for i in xrange(npoint):
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


def splitter_amplification(ch2, chan):
    """Amplification of Stastny's splitter
ch2 - channel 2 of AFG (on/off or True/False)
chan - channel on UUB (1-10)"""
    if str(ch2).lower() in ('on', 'off'):
        ch2 = str(ch2).lower() == 'on'
    if ch2:
        return 4.0 if chan == 9 else 1.0
    else:
        return 1.0/32


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
    values = [int(YSCALE * func(i/scale) + 0.5) for i in xrange(npoint)]
    values = [YSCALE if val > YSCALE else 0 if val < 0 else val
              for val in values]
    preview = [int(0xFF * func(ii/pscale) + 0.5) for ii in xrange(nprev)]
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

if __name__ == '__main__':
    writeTFW(halfsine, 'halfsine', 5000, 5000/(2*math.pi * 10))
