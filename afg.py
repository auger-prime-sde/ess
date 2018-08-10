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
    userfun = """\
output1:state off
source1:function user{usernum:d}
source1:frequency {freq:f}Hz
source1:burst:mode triggered
source1:burst:state ON
source1:burst:ncycles {ncycles:d}
source1:burst:tdelay 0
source1:voltage:level:immediate:low 0
source1:voltage:unit Vpp
output1:impedance 50 Ohm
output1:polarity {polarity:s}
source2:function:shape square
source2:frequency 2kHz
source2:voltage:level:immediate:low -2.5V
source2:voltage:level:immediate:high 2.5V
output2:impedance 50 Ohm
output2:state off
trigger:sequence:source timer
trigger:sequence:timer {timer:f}s
"""
    seton = """\
source1:voltage:level:immediate:high {voltage:f}V
output2:state {ch2:s}
output1:state on
"""
    setoff = """\
output2:state off
output1:state off
"""

    def __init__(self, tmcid=1, zLoadUserfun=False, **kwargs):
        """Constructor.
tmcid - number of usbtmc device (default 1, i.e. /dev/usbtmc1)
zLoadUserfun - if True, load halfsine as a user function
kwargs - parameters:
  period - duration of signal in seconds (default 12.5 us ~ 80kHz)
  usernum - user function number (default 4)
  repetition - repetition period in seconds (default 1ms)
"""
        self.logger = logging.getLogger('AFG')
        device = '/dev/usbtmc%d' % tmcid
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
            raise
        # default parameters
        self.param = {
            'usernum': 4,   # user<d> function
            'freq': 80.e3,  # [Hz] inverse of user function duration
            'ncycles': 1,   # source:burst:ncycles
            'polarity': 'inverted',  # ch1 polarity: normal|inverted
            'timer': 1.0e-3  # [s] period for trigger
            }
        self.voltage = 1.6  # [V] default amplitude
        self.setParams(kwargs)
        # takes about 15s
        if zLoadUserfun:
            self.writeUserfun(halfsine, self.param['usernum'],
                              5000, 5000/(20*math.pi))

    def __del__(self):
        self.logger.info('Closing')
        os.close(self.fd)

    def send(self, line, lvl=logging.DEBUG):
        """Send line to AFG
lvl - optional logging level
"""
        line.rstrip()
        self.logger.log(lvl, 'Sending %s', line)
        os.write(self.fd, line)

    def setParams(self, d=None):
        """Set AFG parameters according to dictionary d.
Updates self.param and send them to AFG."""
        if d:
            for key, val in d.iteritems():
                if key in self.param:
                    self.logger.info('Updating param %s = %s', key, repr(val))
                    self.param[key] = val
                else:
                    self.logger.debug('Param %s = %s ignored', key, repr(val))
        paramlines = AFG.userfun.format(**self.param)
        for line in paramlines.splitlines():
            self.send(line)

    def setOn(self, ch2=True, voltage=None):
        """Switch on signal
ch2 - set channel2 On/Off
voltage - [V], also set voltage if not None
"""
        if voltage is not None:
            self.voltage = voltage
        if str(ch2).lower() not in ('on', 'off'):
            ch2 = 'on' if ch2 else 'off'
        paramlines = AFG.seton.format(ch2=ch2, voltage=self.voltage)
        for line in paramlines.splitlines():
            self.send(line)

    def setOff(self):
        """Switch off signals"""
        for line in AFG.setoff.splitlines():
            self.send(line)

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
        # values = [int(0x4000 * func(i/scale) + 0.5) for i in xrange(npoint)]
        # values = [0x3FFF if val > 0x3FFF else 0 if val < 0 else val
        #         for val in values]
        # datalen = '%d' % (2*npoint)
        # header = 'data:data ememory,#%d%s' % (len(datalen), datalen)
        # self.logger.debug('Writing user funcetion: %s + %s data bytes',
        #                    header, datalen)
        # os.write(self.fd, header + ''.join([pack('>H', x) for x in values]))
        # line = 'data:copy user%d,ememory' % usernum
        # self.logger.debug('Sending %s', line)
        # os.write(self.fd, line)

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
