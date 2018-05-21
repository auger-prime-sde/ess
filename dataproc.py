"""
 ESS procedure
 data processor
"""

import re
import threading
import logging
import itertools
from Queue import Empty
import numpy

# ESS stuff
import hsfitter
from afg import splitter_amplification

class DataProcessor(threading.Thread):
    """Generic data processor"""
    id_generator = itertools.count()
    stop = threading.Event()
    timeout = 1.0
    workhorses = []
    def __init__(self, q_dp):
        super(DataProcessor, self).__init__()
        self.myid = next(self.id_generator)
        self.q_dp = q_dp
        logger = logging.getLogger('DP%d' % self.myid)
        logger.debug('init finished')

    def run(self):
        logger = logging.getLogger('DP%d' % self.myid)
        while not self.stop.is_set():
            try:
                item = self.q_dp.get(True, self.timeout)
            except Empty:
                continue
            logger.debug('processing %s', item2label(item))
            for wh in self.workhorses:
                wh.calculate(item)
        logger.info('run finished')

def item2label(item, **kwargs):
    """Construct label/name for q_resp from item"""
    attr = []
    if 'uubnum' in item:
        attr.append('u%04d' % item['uubnum'])
    if 'chan' in kwargs:
        # transform chan 10 -> c0
        attr.append('c%d' % (kwargs['chan'] % 10))
    if 'voltage' in item:
        attr.append('v%02d' % int(item['voltage'] * 10.))
    if 'ch2' in item:
        attr.append('a%1d' % (item['ch2'] == 'on'))
    return '_'.join(attr)

re_label = re.compile(r'(?P<type>[a-z]+)_' +
                      r'u(?P<uubnum>\d{4})_' +
                      r'c(?P<chan>\d)_' +
                      r'v(?P<voltage>\d\d)_' +
                      r'a(?P<ch2>\d)')
def label2item(label):
    """Check if label stems from item and parse it to components"""
    m = re_label.match(label)
    if m is None:
        return None
    d = m.groupdict()
    d.update({key: int(d[key]) for key in ('uubnum', 'chan')})
    # convert voltage back to float and chan 0 -> 10
    d['voltage'] = 0.1 * float(d['voltage'])
    if d['chan'] == 0:
        d['chan'] = 10
    d['ch2'] = d['ch2'] == '1'
    return d

class DP_pede(object):
    """Data processor workhorse to calculate pedestals"""
    # parameters
    BINSTART = 50
    BINEND = 550
    def __init__(self, q_resp, it2label):
        """Constructor.
q_resp - a logger queue
it2label - a function to generate names
"""
        self.q_resp = q_resp
        self.it2label = it2label

    def calculate(self, item):
        array = item['yall'][self.BINSTART:self.BINEND, :]
        mean = array.mean(axis=0)
        stddev = array.std(axis=0)
        res = {'timestamp': item['timestamp']}
        if 'meas_point' in item:
            res['meas_point'] = item['meas_point']
        for ch, (m, s) in enumerate(zip(mean, stddev)):
            label = self.it2label(item, chan=ch+1)
            res['pede_' + label] = m
            res['pedesig_' + label] = s
        self.q_resp.put(res)

class DP_hsampli(object):
    """Data processor workhorse to calculate amplitude of half-sines"""
    # parameters
    def __init__(self, q_resp, it2label, w):
        """Constructor.
q_resp - a logger queue
it2label - a function to generate names
w - width of half-sine in us
"""
        self.q_resp = q_resp
        self.it2label = it2label
        self.hsf = hsfitter.HalfSineFitter(w)

    def calculate(self, item):
        hsfres = self.hsf.fit(item['yall'], hsfitter.AMPLI)
        res = {'timestamp': item['timestamp']}
        if 'meas_point' in item:
            res['meas_point'] = item['meas_point']
        for ch, ampli in enumerate(hsfres['ampli']):
            label = self.it2label(item, chan=ch+1)
            res['ampli_' + label] = ampli
        self.q_resp.put(res)

def dpfilter_linear(res_in):
    """Dataprocessing filter
 - calculate linear fit & correlation coeff from ampli
input items: ampli_u<uubnum>_c<uub channel>_v<voltage>_a<afg.ch2>
data: x - real voltage amplitude after splitter [mV], y - UUB ADCcount amplitude
output items: sens_u<uubnum>_c<uub channel> - sensitivity: ADC counts / mV
              r_u<uubnum>_c<uub channel> - correlation coefficient
"""
    lowgain = (1, 3, 5, 7, 9)  # UUB low and low-low gain channels
    highgain = (2, 4, 6, 10)   # UUB high gain channels except not connected 8
    data = {}
    for label, adcvalue in res_in.iteritems():
        d = label2item(label)
        if d is None or d['type'] != 'ampli':
            continue
        chan, ch2 = d['chan'], d['ch2']
        if not(chan in lowgain or chan in highgain and not ch2):
            continue
        # voltage in mV
        voltage = 1000*d['voltage'] * splitter_amplification(ch2, chan)
        key = (d['uubnum'], chan)
        if key not in data:
            data[key] = []
        data[key].append((voltage, adcvalue))
    res_out = res_in.copy()
    for key, xy in data.iteritems():
        # xy = [[v1, adc1], [v2, adc2] ....]
        xy = numpy.array(xy)
        # xx_xy = [v1, v2, ...] * xy
        xx_xy = xy.T[0].dot(xy)
        slope = xx_xy[1] / xx_xy[0]
        covm = numpy.cov(xy, rowvar=False)
        # correlation coeff = cov(V, ADC) / sqrt(var(V) * var(ADC))
        if xy.shape[0] > 1:
            coeff = covm[0][1] / numpy.sqrt(covm[0][0] * covm[1][1])
        else:
            coeff = 0.0
        label = item2label({'uubnum': key[0]}, chan=key[1])
        res_out['sens_'+label] = slope
        res_out['r_'+label] = coeff
    return res_out
