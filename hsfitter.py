"""
 ESS procedure
 fit by sine and halfsine model functions
"""

from math import pi
from numpy import arange, linspace, unwrap, concatenate
from numpy import fft, angle, zeros, ones, vander
from numpy import dot, outer, matmul, linalg
from numpy import sqrt, sin, cos, arctan2
import numba

from dataproc import float2expo, expo2float


# squared norm of complex array
@numba.vectorize([numba.float64(numba.complex128),
                  numba.float32(numba.complex64)])
def abs2(x):
    return x.real**2 + x.imag**2


class HalfSineFitter(object):
    """Fit a train of half sine pulses using FFT"""
    (AMPLI, PEDE, PHASE, YVAL) = range(4)

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
        mask = (argsine % (4*pi) < pi) & (argsine > 0) & \
               (argsine < self.Npeak*4*pi)
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
        if stage == HalfSineFitter.AMPLI:
            return res

        # calculate pedestals
        pede = (abs(yfft[0, :]) - ampli * self.c0) / self.N

        res['pede'] = pede
        if stage == HalfSineFitter.PEDE:
            return res

        # calculate binstart
        mphase = outer(self.mphase, ones(Ncol))
        phasedif = unwrap(angle(yfft[:self.Nphase, :]) - mphase, axis=0)
        slope = dot(self.abs2[:self.Nphase], phasedif) / self.normphase
        binstart = self.binstart - N/2/pi * slope

        res['binstart'] = binstart
        if stage == HalfSineFitter.PHASE:
            return res

        # calculate function values
        yf = zeros((self.N, Ncol))
        for i in range(Ncol):
            yf[:, i] = self.halfsine(ampli=ampli[i], binstart=binstart[i],
                                     pede=pede[i])

        res['yval'] = yf
        return res


class SineFitter(object):
    """Fit sine plus decaying baseline"""
    (AMPLI, PARAM, CHI, YFIT) = range(4)
    YMAX = 4095

    def __init__(self, N=2048, FREQ=120., NPOLY=1):
        # fixed parameters
        self.N = N             # number of bins
        self.FREQ = FREQ       # ADC sampling rate in MHz
        self.NPOLY = NPOLY     # degree of baseline polynomial
        self.freqs = {}
        x = arange(N, dtype='float64')
        self.vander = vander(x, NPOLY+1, True)
        self.x = x.reshape(N, 1)

    def addFreq(self, flabel, freq=None):
        """Add a frequncy and precompute sine and cosine arrays for it
flabel - freq converted to expo representation
freq - frequency of sine in Hz
"""
        if freq is None:
            freq = expo2float(flabel)
        elif flabel is None:
            flabel = float2expo(freq)
        if flabel in self.freqs:
            return
        omega = 2*pi/self.FREQ * freq/1.e6
        matX = concatenate((cos(omega*self.x), sin(omega*self.x), self.vander),
                           axis=1)
        self.freqs[flabel] = matX

    def fit(self, yall, flabel, stage=YFIT):
        """Perform fit
yall  - array(2048, Ncol)
stage - what to calculate: AMPLI, PARAM, CHI, YVAL
return dict with keys: ampli, param, chi, yval
        param contains amplitude, phase and polynomial"""
        N, Ncol = yall.shape
        assert N == self.N
        self.addFreq(flabel)
        matX = self.freqs[flabel]
        res = {'ampli': zeros(Ncol)}
        if stage >= SineFitter.PARAM:
            res['param'] = zeros((Ncol, 3+self.NPOLY))
        if stage >= SineFitter.CHI:
            res['chi'] = zeros(Ncol)
        if stage >= SineFitter.YFIT:
            res['yfit'] = zeros((self.N, Ncol))
        for col in xrange(Ncol):
            y = yall[:, col]
            ind = (0 < y) & (y < SineFitter.YMAX)
            ind4095 = y >= SineFitter.YMAX
            matX1 = matX[ind]
            y1 = y[ind]
            N1 = y1.shape[0]
            matM = linalg.inv(matmul(matX1.T, matX1)/N1)
            b = matmul(matX1.T, y1)/N1
            a = matmul(matM, b)
            res['ampli'][col] = sqrt(a[0]*a[0] + a[1]*a[1])
            if stage >= SineFitter.PARAM:
                params = zeros(3+self.NPOLY)
                params[0] = res['ampli'][col]
                params[1] = arctan2(a[1], a[0])  # phase
                params[2:] = a[2:]
                res['param'][col, :] = params
            if stage >= SineFitter.CHI:
                res['chi'][col] = sqrt(dot(y1, y1)/N1 - dot(a, b))
            if stage >= SineFitter.YFIT:
                res['yfit'][ind4095, col] = SineFitter.YMAX
                res['yfit'][ind, col] = matmul(matX1, a)
        return res
