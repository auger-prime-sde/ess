"""
  Communication with Tektronix MDO3000/DPO7000
"""

import logging
import os
import errno
import re
import socket
from struct import unpack
import numpy as np

from afg import TekDevice, BlackHoleLogger


class MDO(TekDevice):
    """Class for data readout from (TekTronix mainly) oscilloscopes"""

    PARAM = {'WFMOUTPRE:BYT_NR': 1,
             'DATA:ENCDG': 'RIBINARY'}
    STRPARAM = ('WFMOUTPRE:BYT_OR', 'WFMOUTPRE:BN_FMT', 'WFMOUTPRE:ENCDG',
                'DATA:ENCDG')
    NUMPARAM = ('WFMOUTPRE:BYT_NR', 'WFMOUTPRE:BIT_NR')
    ENDIAN = {'MSB': '>', 'LSB': '<'}
    NRTYPE = {'8': 'b', '16': 'h'}

    def __init__(self, device, **kwargs):
        self.logger = logging.getLogger('MDO')
        super(MDO, self).__init__(device, self.logger)
        self.setParams(**MDO.PARAM)
        params = {key: kwargs[key] for key in MDO.NUMPARAM + MDO.STRPARAM
                  if key in kwargs}
        self.setParams(**params)

    def setParams(self, **d):
        """Set MDO parameters according to dict <d>"""
        for key, val in d.items():
            if key in MDO.NUMPARAM:
                self.send('%s %d' % (key, val))
            elif key in MDO.STRPARAM:
                self.send('%s %s' % (key, val))

    def _parseWFM(self, resp):
        """Parse response to :WFMOUTPRE?"""
        d = dict([item.split(' ', 1)
                  for item in resp.split(';')])
        assert d['ENCDG'] == 'BINARY'
        assert d['BN_FMT'] == 'RI'
        return {key: val for key, val in d.items()
                if key in ('BYT_OR', 'BIT_NR',
                           'XUNIT', 'XZERO', 'XINCR',
                           'YUNIT', 'YZERO', 'YMULT', 'YOFF')}

    def readWFM(self, ch, dataslice=None, fn=None):
        """Read waveform from oscilloscope
ch - oscilloscope channel to read
dataslice - optional (start, stop, step) to slice acquired data
fn - if not None, save data (raw format) to the file
return tuple (numpy.array yvals, float xincr, float xzero, xunit, yunit)"""
        self.send('DATA:SOURCE CH%d' % ch)
        self.send('HEADER 1')
        resp = self.send('WFMOUTPRE?', 1000)
        self.logger.debug('WFM: %s', resp)
        wfmd = self._parseWFM(resp)
        self.send('HEADER 0')
        self.send('CURVE?')
        h = self.read(2, False)
        assert h[0] == ord('#')
        numpt = int(self.read(h[1] - ord('0'), False))
        data = self.read(numpt, False)
        self.read(1)  # EOL
        self.logger.debug('WFM transferred')
        ndata = numpt // (int(wfmd['BIT_NR']) // 8)
        fmtstr = (MDO.ENDIAN[wfmd['BYT_OR']] + str(ndata) +
                  MDO.NRTYPE[wfmd['BIT_NR']])
        yraw = unpack(fmtstr, data)
        xincr, xzero = [float(wfmd[k]) for k in ('XINCR', 'XZERO')]
        if dataslice is not None:
            start, stop, step = dataslice
            yraw = yraw[start:stop:step]
            if start is not None:
                xzero += xincr*start
            if step is not None:
                xincr *= step
        if fn is not None:
            wfmd['XZERO'] = xzero
            wfmd['XINCR'] = xincr
            prolog = """\
# Waveform from MDO, raw data
# x_i = XZERO + i * XINCR
# y_i = YZERO + YMULT * ( raw_i - YOFF )
# XZERO = {XZERO:f}; XINCR = {XINCR:f}
# YZERO = {YZERO:s}; YMULT = {YMULT:s}; YOFF = {YOFF:s}
# XUNIT: {XUNIT:s}, YUNIT: {YUNIT:s}
""".format(**wfmd)
            with open(fn, 'w') as fp:
                fp.write(prolog)
                for y in yraw:
                    fp.write(str(y) + '\n')
            self.logger.debug('raw data saved to %s', fn)
        yvals = np.array(yraw) - float(wfmd['YOFF'])
        yvals = float(wfmd['YZERO']) + float(wfmd['YMULT'])*yvals
        xunit, yunit = [wfmd[k].strip('"') for k in ('XUNIT', 'YUNIT')]
        self.logger.debug('readWFM done')
        return (yvals, xincr, xzero, xunit, yunit)
