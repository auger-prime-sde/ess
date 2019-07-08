"""
 ESS procedure
 fit by sine and halfsine model functions
"""

from math import pi
import numpy as np

try:
    import numba

    # squared norm of complex array
    @numba.vectorize([numba.float64(numba.complex128),
                      numba.float32(numba.complex64)])
    def abs2(x):
        return x.real**2 + x.imag**2
except ImportError:
    # use np.vectorize as backup solution
    def _abs2(x):
        return x.real**2 + x.imag**2
    abs2 = np.vectorize(_abs2)
    del _abs2


class HalfSineFitter(object):
    """Fit a train of half sine pulses using FFT"""
    (AMPLI, PEDE, PHASE, YVAL, CHI) = range(5)

    def __init__(self, w, N=2048, FREQ=120., Npeak=5, zInvert=False):
        self.w = w             # half period of sine in us
        # fixed parameters
        self.N = N             # number of bins
        self.FREQ = FREQ       # ADC sampling rate in MHz
        self.Npeak = Npeak     # how many halfsines
        self.zInvert = zInvert  # halfsines negative?
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
        if self.zInvert:
            ampli = -ampli
        argsine = pi/self.w/self.FREQ * (
            np.linspace(0, self.N, self.N, endpoint=False) - binstart)
        mask = (argsine % (4*pi) < pi) & (argsine > 0) & \
               (argsine < self.Npeak*4*pi)
        res = np.zeros(self.N) + pede
        res[mask] += ampli * np.sin(argsine[mask])
        return res

    def _calc_model(self):
        """Calculate model data and its fft"""
        assert self.Nampli >= self.Nphase
        self.y = self.halfsine()      # model
        yfft = np.fft.fft(self.y, axis=0)
        self.abs2 = abs2(yfft[:self.Nampli])   # norm of coefficients
        self.power = np.dot(self.abs2[1:], self.abs2[1:])
        self.c0 = np.real(yfft[0])
        self.mphase = np.angle(yfft[1:self.Nphase])
        self.normphase = np.dot(np.arange(1, self.Nphase),
                                self.abs2[1:self.Nphase])

    def fit(self, yall, stage=YVAL):
        """Perform fit
yall  - array(2048, Ncol)
stage - what to calculate: AMPLI, PEDE, PHASE, YVAL"""
        N, Ncol = yall.shape
        assert N == self.N
        # calculate amplitudes
        yfft = np.fft.fft(yall, axis=0)
        yabs = abs2(yfft[1:self.Nampli, :])
        ampli = np.sqrt(np.dot(self.abs2[1:], yabs) / self.power)

        res = {'ampli': ampli}
        if stage == HalfSineFitter.AMPLI:
            return res

        # calculate pedestals
        pede = (np.real(yfft[0, :]) - ampli * self.c0) / self.N

        res['pede'] = pede
        if stage == HalfSineFitter.PEDE:
            return res

        # calculate binstart
        mphase = np.outer(self.mphase, np.ones(Ncol))
        phasedif = np.unwrap(np.angle(yfft[1:self.Nphase, :]) - mphase, axis=0)
        slope = np.dot(self.abs2[1:self.Nphase], phasedif) / self.normphase
        binstart = self.binstart - N/2/pi * slope

        res['binstart'] = binstart
        if stage == HalfSineFitter.PHASE:
            return res

        # calculate function values
        yf = np.zeros((self.N, Ncol))
        for i in range(Ncol):
            yf[:, i] = self.halfsine(ampli=ampli[i], binstart=binstart[i],
                                     pede=pede[i])

        res['yval'] = yf
        if stage == HalfSineFitter.YVAL:
            return res

        # calculate chi
        res['chi'] = np.std(yall - yf, axis=0)
        return res


class SineFitter(object):
    """Fit sine plus decaying baseline"""
    (AMPLI, PARAM, CHI, YFIT) = range(4)
    YMAX = 4095

    def __init__(self, N=2048, FREQ=120., NPOLY=1, NHARM=1):
        # fixed parameters
        self.N = N             # number of bins
        self.FREQ = FREQ       # ADC sampling rate in MHz
        self.NPOLY = NPOLY     # degree of baseline polynomial
        self.NHARM = NHARM     # number of harmonics
        self.freqs = {}
        x = np.arange(N, dtype='float64')
        # vander: vandermont matrix normalized to 1
        self.vander = np.ones((N, NPOLY+1))
        if NPOLY >= 1:
            self.vander[:, 1] = (2*x + 1.0)/N - 1.0
            for i in xrange(2, NPOLY):
                self.vander[:, i] = self.vander[:, i-1] * self.vander[:, 1]
#        self.x = x.reshape(N, 1)
        self.x = x
        self.crop = True

    def addFreq(self, flabel, freq):
        """Add a frequncy and precompute sine and cosine arrays for it
flabel - freq converted to expo representation
freq - frequency of sine in Hz
return matrix freqs[flabel]
"""
        if flabel in self.freqs:
            return self.freqs[flabel]
        omega = 2*pi/self.FREQ * freq/1.e6
        matX = np.zeros((self.N, 2*self.NHARM + self.NPOLY+1))
        for n in xrange(self.NHARM):
            matX[:, 2*n] = np.cos((n+1)*omega*self.x)
            matX[:, 2*n+1] = np.sin((n+1)*omega*self.x)
        matX[:, 2*self.NHARM:] = self.vander
        self.freqs[flabel] = matX
        return matX

    def fit(self, yall, flabel, freq, stage=YFIT):
        """Perform fit
yall  - array(2048, Ncol)
freq - frequency of sine in Hz
stage - what to calculate: AMPLI, PARAM, CHI, YVAL
return dict with keys: ampli, param, chi, yval
        param contains amplitude, phase and polynomial"""
        N, Ncol = yall.shape
        assert N == self.N
        matX = self.addFreq(flabel, freq)
        res = {'ampli': np.zeros(Ncol)}
        if stage >= SineFitter.PARAM:
            res['param'] = np.zeros((Ncol, 2*self.NHARM + 1 + self.NPOLY))
        if stage >= SineFitter.CHI:
            res['chi'] = np.zeros(Ncol)
        if stage >= SineFitter.YFIT:
            res['yfit'] = np.zeros((self.N, Ncol))
        for col in xrange(Ncol):
            y = yall[:, col]
            if self.crop:
                ind = (0 < y) & (y < SineFitter.YMAX)
                ind4095 = y >= SineFitter.YMAX
            else:
                ind = np.full_like(y, True, dtype=bool)
                ind4095 = np.full_like(y, False, dtype=bool)
            matX1 = matX[ind]
            y1 = y[ind]
            N1 = y1.shape[0]
            matM = np.linalg.inv(np.matmul(matX1.T, matX1)/N1)
            b = np.matmul(matX1.T, y1)/N1
            a = np.matmul(matM, b)
            res['ampli'][col] = np.sqrt(a[0]*a[0] + a[1]*a[1])
            if stage >= SineFitter.PARAM:
                res['param'][col, :] = a
            if stage >= SineFitter.CHI:
                res['chi'][col] = np.sqrt(np.dot(y1, y1)/N1 - np.dot(a, b))
            if stage >= SineFitter.YFIT:
                res['yfit'][ind4095, col] = SineFitter.YMAX
                res['yfit'][ind, col] = np.matmul(matX1, a)
        return res
