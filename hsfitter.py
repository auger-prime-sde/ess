from math import pi
from numpy import sqrt, sin, arange, linspace, unwrap
from numpy import fft, angle, dot, outer, zeros, ones
from numpy import sum as npsum
import numba

# squared norm of complex array
@numba.vectorize([numba.float64(numba.complex128),
                  numba.float32(numba.complex64)])
def abs2(x):
    return x.real**2 + x.imag**2

(AMPLI, PEDE, PHASE, YVAL) = range(4)

class HalfSineFitter(object):
    """Fit a train of half sine pulses using FFT"""
    def __init__(self, w, N=2048, FREQ=120., Npeak=5):
        self.w = w             # half period of sine in us
        # fixed parameters
        self.N = N             # number of bins
        self.FREQ = FREQ       # ADC sampling rate in MHz
        self.Npeak = Npeak     # how many halfsines
        # parameters for fitting, set before calling model()
        self.binstart = 600
        self.Nampli = 200      # cut on fft coefs for calculation of amplitudes
        self.Nphase = 100      # cut on fft coefs for calculation of phases
        # init model
        self._calc_model()

    def halfsine(self, ampli=1, pede=0, binstart=None):
        """
Calculate train of half sine pulses of width w, separated by 3*w pedestal
    ampli    - amplitude of sine [ADC counts]
    pede     - pedestal offset [ADC counts]
    binstart - time offset [bins]
    """
        if binstart is None:
            binstart = self.binstart
        argsine = pi/self.w/self.FREQ * (
            linspace(0, self.N, self.N, endpoint=False) - binstart)
        mask = (argsine % (4*pi) < pi) & (argsine > 0) \
               & (argsine < self.Npeak*4*pi)
        res = zeros(self.N) + pede
        res[mask] += ampli * sin(argsine[mask])
        return res
        
    def _calc_model(self):
        """Calculate model data and its fft"""
        assert self.Nampli >= self.Nphase
        self.y = self.halfsine()      # model 
        yfft = fft.fft(self.y, axis=0)
        self.abs2 = abs2(yfft[:self.Nampli])   # norm of coefficients
        self.power = dot(self.abs2[1:], self.abs2[1:])
        self.c0 = abs(yfft[0])
        self.mphase = angle(yfft[:self.Nphase])
        self.normphase = dot(arange(0, self.Nphase), self.abs2[:self.Nphase])

    def fit(self, yall, stage=YVAL):
        """Perform fit
yall  - array(2048, Ncol)
stage - what to calculate: AMPLI, PEDE, PHASE, YVAL"""
        N, Ncol = yall.shape
        assert N == self.N
        # calculate amplitudes
        yfft = fft.fft(yall, axis=0)
        yabs = abs2(yfft[1:self.Nampli, :])
        ampli = sqrt(dot(self.abs2[1:], yabs) / self.power)

        res = {'ampli': ampli}
        if stage == AMPLI:
            return res

        # calculate pedestals
        pede = ( abs(yfft[0, :]) - ampli * self.c0 ) / self.N

        res['pede'] = pede
        if stage == PEDE:
            return res

        # calculate binstart
        mphase = outer(self.mphase, ones(Ncol))
        phasedif = unwrap(angle(yfft[:self.Nphase, :]) - mphase, axis=0)
        slope = dot(self.abs2[:self.Nphase], phasedif) / self.normphase
        binstart = self.binstart - N/2/pi * slope 

        res['binstart'] = binstart
        if stage == PHASE:
            return res

        # calculate function values
        yf = zeros((self.N, Ncol))
        for i in range(Ncol):
            yf[:, i] = self.halfsine(ampli=ampli[i], binstart=binstart[i],
                                     pede=pede[i])

        res['yval'] = yf
        return res
