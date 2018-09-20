"""
 ESS procedure
 data processor
"""

import re
import threading
import logging
import string
import itertools
from Queue import Empty
from datetime import datetime
import numpy

# ESS stuff
import hsfitter
from afg import splitter_amplification


def float2expo(x, manlength=3):
    """Convert float x to E M1 M2 .. Mn, x = M1.M2 ... Mn * 10^E
manlength - maximal length of mantisa (n)
Return str EM1M2...Mn"""
    x = float(x)
    assert x >= 0, "x must be non-negative"
    eps = 5. / 10 ** manlength
    for expo in range(10):
        if x + eps < 10.:
            break
        x /= 10
    else:
        raise AssertionError("x too big")
    imant = int((x + eps) * 10 ** (manlength-1))  # mantisa as int
    mant = string.rstrip('%d' % imant, '0')
    return '%d%s' % (expo, mant)


def expo2float(s):
    """Convert E M1 M2 .. Mn to float"""
    assert re.match(r'^\d\d+$', s), 'Wrong EMMM format'
    expo = int(s[0])
    imant = float(s[1:])
    return imant * 10 ** (expo - (len(s) - 2))


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
                try:
                    wh.calculate(item)
                except Exception as e:
                    logger.error('Workhorse %s with item = %s failed',
                                 repr(wh), repr(item))
                    logger.exception(e)
        logger.info('run finished')


LABEL_DOC = """ Label to item (and back) conversion
label = attr1_attr2 ... _attrn<functype>
attr are (optional, but in this order):
  attr       re in label   python in item       meaning
----------------------------------------------------------
  typ        [a-z]+      str              ampli, pede, pedesig ...
  timestamp  \d{14}      datetime         YYYYmmddHHMMSS
  uubnum     u\d{4}      int 0-9999       u0015
  chan       c\d         int 1-10         c1 - c9, c0 .. channels 1 - 10
  ch2        a[01]       False/True       splitter ch2 on/off
  voltage    v\d{2,3}    float            voltage coded as v1.v2v3 [volt]
  freq       f\d{2,4}    float            EM1M2M3 coded freq M1.M2M3*10^E Hz
  functype   [A-Z]       char             P - pulse series, F - sine

mandatory keys: uubnum, ch2, voltage/freq, functype
optional keys (typ, timestamp, chan)
"""


def item2label(item=None, **kwargs):
    """Construct label/name for q_resp from item and kwargs
kwargs and item are merged, item is not modified"""
    if item is None:
        item = {}
    if kwargs:
        kwargs.update(item)
    else:
        kwargs = item
    attr = []
    if 'typ' in kwargs:
        attr.append(kwargs['typ'])
    if 'timestamp' in kwargs:
        attr.append(kwargs['timestamp'].strftime('%Y%m%d%H%M%S'))
    if 'uubnum' in kwargs:
        attr.append('u%04d' % kwargs['uubnum'])
    if 'chan' in kwargs:
        # transform chan 10 -> c0
        attr.append('c%d' % (kwargs['chan'] % 10))
    if 'ch2' in kwargs:
        attr.append('a1' if kwargs['ch2'] else 'a0')
    functype = kwargs.get('functype', '')
    if 'voltage' in kwargs:
        svolt = 'v%03d' % int(kwargs['voltage'] * 100.)
        if(svolt[-1] == '0'):
            svolt = svolt[:-1]
        attr.append(svolt)
    if functype == 'F':
        if 'freq' in kwargs:
            attr.append('f' + float2expo(kwargs['freq'], manlength=3))
    return '_'.join(attr) + functype


re_labels = [re.compile(regex) for regex in (
    r'(?P<typ>[a-z]+)',
    r'(?P<timestamp>20\d{12})',
    r'u(?P<uubnum>\d{4})',
    r'c(?P<chan>\d)',
    r'a(?P<ch2>[01])',
    r'v(?P<voltage>\d{2,3})',
    r'f(?P<freq>\d{2,4})')]


def label2item(label):
    """Check if label stems from item and parse it to components"""
    if 'A' <= label[-1] <= 'Z':
        d = {'functype': label[-1]}
        attrs = label[:-1].split('_')
    else:
        d = {}
        attrs = label.split('_')
    attr = attrs.pop(0)
    for rattr in re_labels:
        m = rattr.match(attr)
        if m is None:
            continue
        d.update(m.groupdict())
        if len(attrs) == 0:
            break
        attr = attrs.pop(0)

    # change strings from re to Python objects
    # uubnum and chan to integers
    d.update({key: int(d[key]) for key in ('uubnum', 'chan') if key in d})
    if 'chan' in d and d['chan'] == 0:
        d['chan'] = 10
    # convert voltage back to float and chan 0 -> 10
    if 'voltage' in d:
        svolt = d['voltage']
        d['voltage'] = float('%c.%s' % (svolt[0], svolt[1:]))
    if 'freq' in d:
        try:
            d['freq'] = expo2float(d['freq'])
        except AssertionError:
            logging.getLogger('label2item').warning(
                'Wrong expo2float argument %s', d['freq'])
            del d['freq']
    if 'ch2' in d:
        d['ch2'] = d['ch2'] == '1'
    if 'timestamp' in d:
        try:
            d['timestamp'] = datetime.strptime(d['timestamp'], '%Y%m%d%H%M%S')
        except ValueError:
            logging.getLogger('label2item').warning(
                'Wrong timestamp %s (label %s)', d['timestamp'], label)
            del d['timestamp']
    return d


class DP_pede(object):
    """Data processor workhorse to calculate pedestals"""
    # parameters
    BINSTART = 50
    BINEND = 550

    def __init__(self, q_resp):
        """Constructor.
q_resp - a logger queue
"""
        self.q_resp = q_resp

    def calculate(self, item):
        if item['functype'] != 'P':
            return
        logging.getLogger('DP_pede').debug(
            'Processing %s', item2label(item))
        array = item['yall'][self.BINSTART:self.BINEND, :]
        mean = array.mean(axis=0)
        stddev = array.std(axis=0)
        res = {'timestamp': item['timestamp']}
        itemr = {key: item[key]
                 for key in ('uubnum', 'voltage', 'ch2', 'functype')}
        for ch, (m, s) in enumerate(zip(mean, stddev)):
            for typ, val in (('pede', m), ('pedesig', s)):
                label = item2label(itemr, typ=typ, chan=ch+1)
                res[label] = val
        self.q_resp.put(res)


class DP_hsampli(object):
    """Data processor workhorse to calculate amplitude of half-sines"""
    # parameters
    def __init__(self, q_resp, hswidth):
        """Constructor.
q_resp - a logger queue
hswidth - width of half-sine in microseconds
"""
        self.q_resp = q_resp
        self.hsf = hsfitter.HalfSineFitter(hswidth)

    def calculate(self, item):
        if item['functype'] != 'P':
            return
        logging.getLogger('DP_hsampli').debug(
            'Processing %s', item2label(item))
        hsfres = self.hsf.fit(item['yall'], hsfitter.AMPLI)
        res = {'timestamp': item['timestamp']}
        itemr = {key: item[key]
                 for key in ('uubnum', 'voltage', 'ch2', 'functype')}
        itemr['typ'] = 'ampli'
        for ch, ampli in enumerate(hsfres['ampli']):
            label = item2label(itemr, chan=ch+1)
            res[label] = ampli
        self.q_resp.put(res)


def dpfilter_linear(res_in):
    """Dataprocessing filter
 - calculate linear fit & correlation coeff from ampli
input items: ampli_u<uubnum>_c<uub channel>_v<voltage>_a<afg.ch2>P
data: x - real voltage amplitude after splitter [mV],
      y - UUB ADCcount amplitude
output items: sens_u<uubnum>_c<uub channel> - sensitivity: ADC counts / mV
              r_u<uubnum>_c<uub channel> - correlation coefficient
"""
    # logging.getLogger('dpfilter_linear').debug('res_in: %s', repr(res_in))
    lowgain = (1, 3, 5, 7, 9)  # UUB low and low-low gain channels
    highgain = (2, 4, 6, 10)   # UUB high gain channels except not connected 8
    data = {}
    for label, adcvalue in res_in.iteritems():
        d = label2item(label)
        if d is None or 'typ' not in d or d['typ'] != 'ampli':
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
        res_out['corr_'+label] = coeff
    return res_out


class DP_store(object):
    """Data processor workhorse to store 2048x10 data"""

    def __init__(self, datadir):
        """Constructor.
"""
        self.datadir = datadir

    def calculate(self, item):
        label = item2label(item)
        logging.getLogger('DP_store').debug('Processing %s', label)
        fn = '%s/dataall_%s.txt' % (self.datadir, label)
        numpy.savetxt(fn, item['yall'], fmt='% 5d')
