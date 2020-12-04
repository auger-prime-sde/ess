

import numpy as np

VERSION = '20190812'


class LinFit(object):
    TYPES = (SIMPLE, ERROR, AGGREG) = range(3)
    REPS = 1.001  # relative error

    def __init__(self, type):
        assert type in LinFit.TYPES, "Unknown type of LinFit"
        self.y = None
        if type == LinFit.AGGREG:
            self._points = []
            setattr(self, "add_point",
                    LinFit._add_point_AGG.__get__(self, LinFit))
            setattr(self, "fini_points",
                    LinFit._fini_points_AGG.__get__(self, LinFit))
        else:
            raise RuntimeError('Not implemented yet')

    def _new_point_AGG(self, x, y):
        """Initialize new point"""
        point = {'x': x, 'xm': x/self.REPS, 'xp': x*self.REPS,
                 'n': 1, 'sumy': y, 'sumyy': y*y}
        return point

    def _fini_points_AGG(self):
        x = [point['x'] for point in self._points]
        y = [point['sumy'] / point['n'] for point in self._points]
        vary = [(point['sumyy'] - Y*Y*point['n'])/(point['n']-1)
                for point, Y in zip(self._points, y)]
        self.x = np.array(x)
        self.y = np.array(y)
        self.vary = np.array(vary)
        del self._points

    def _add_point_AGG(self, x, y):
        candidates = [point for point in self._points
                      if point['xm'] < x < point['xp']]
        assert len(candidates) < 2
        if candidates:
            point = candidates[0]
            point['n'] += 1
            point['sumy'] += y
            point['sumyy'] += y*y
        else:
            self._points.append(self._new_point_AGG(x, y))

    def fit_general(self, xfun, zCorrcoef=False):
        """General fit y = X*a + eps
implicit inputs: self.x, self.y, self.vary - vectors
xfun - function to generate rows of X: x -> X_i
zCorrcoef - if True, suppose that xfun(x) = x and calculate correlation coeff.
return: (a, vara, chi2, nf, cc or None), where
  a - result vector
  vara - variance matrix of a
  chi2 - chi square normalized to vary
  nf - degree of freedom
  cc - complement to correlation coefficient (1.0 - corr. coef.)
"""
        if self.y is None:
            self.fini_points()
        ny = self.y.shape[0]
        nx = len(xfun(0))
        M = np.empty((ny, nx+1))
        M[:, 0] = self.y
        for i in range(ny):
            M[i, 1:] = xfun(self.x[i])
        vary = self.vary.reshape((ny, 1))
        MtCM = np.dot(M.T, np.multiply(M, 1.0/vary))
        vara = np.linalg.inv(MtCM[1:, 1:])
        a = np.dot(vara, MtCM[1:, 0])
        stda = np.sqrt(np.diag(vara))
        cova = vara * np.outer(1/stda, 1/stda)
        chi = MtCM[0, 0] - np.dot(MtCM[0, 1:], a)
        if zCorrcoef:
            cc = 1.0 - MtCM[0, 1]/np.sqrt(MtCM[0][0] * MtCM[1][1])
        else:
            cc = None
        return a, stda, cova, chi, ny - nx, cc

    def eval_func(self, xfun, a):
        """Evaluate X*a
implicit inputs: self.x, self.y, self.vary - vectors
xfun - function to generate rows of X: x -> X_i
a - result of fit_general"""
        ny = self.y.shape[0]
        X = np.array([xfun(self.x[i]) for i in range(ny)])
        return np.dot(X, a)
