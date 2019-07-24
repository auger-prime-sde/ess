"""
 ESS procedure
 data processor
"""

import re
import multiprocessing
import logging
import json
from datetime import datetime
import numpy as np

# ESS stuff
from hsfitter import HalfSineFitter, SineFitter


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
    mant = ('%d' % imant).rstrip('0')
    return '%d%s' % (expo, mant)


def expo2float(s):
    """Convert E M1 M2 .. Mn to float"""
    assert re.match(r'^\d\d+$', s), 'Wrong EMMM format'
    expo = int(s[0])
    imant = float(s[1:])
    return imant * 10 ** (expo - (len(s) - 2))


class SplitterGain(object):
    """Gain of Stastny's splitter"""
    # mapping UUB chan to splitchan + high gain (True)
    UUB2SPLIT = (None,  # placeholder to allow chan 1 .. 10
                 ('A', False), ('A', True),
                 ('B', False), ('B', True),
                 ('C', False), ('C', True),
                 ('D', False),
                 ('E', False),
                 ('F', False), ('F', True))

    def __init__(self, pregains=(1, None), mdochans=None, uubnums=None,
                 calibration=None):
        """Constructor.
pregain - afg.gains for splitter input (float)
mdochans - list of upto 4 splitter channels
uubnums - list of upto 10 UUB numbers
calibration - dict(key: correction_value),
              key: "%d%s%s" % (splitmode, splitch, flabel)
              correction_value: 1.0 in ideal case
"""
        assert len(pregains) == 2
        self.pregains = [float(p) if p is not None else None
                         for p in pregains]
        self.mdomap = None
        if mdochans is not None:
            assert 0 < len(mdochans) <= 4
            self.mdomap = {ch+1: str(splitch)
                           for ch, splitch in enumerate(mdochans)
                           if splitch is not None and (
                                   len(splitch) == 2 and
                                   splitch[0] in 'ABCDEF' and
                                   splitch[1] in '0123456789') or
                           splitch == 'REF'}
            if any([len(splitch) == 2 for splitch in self.mdomap.values()]):
                assert self.pregains[0] is not None
            if 'REF' in self.mdomap.values():
                assert self.pregains[1] is not None
        if uubnums is not None:
            assert len(uubnums) < 10 and \
                all([0 < uubnum < 4000
                     for uubnum in uubnums if uubnum is not None])
            self.uubnums = uubnums
        else:
            self.uubnums = [None] * 10
        if calibration is not None:
            self.calibration = json.load(file(calibration))
        else:
            self.calibration = {}

    def gainMDO(self, splitmode, mdoch, flabel=""):
        """Return gain for MDO channel
mdoch - 1 .. 4"""
        return self._gain(splitmode, self.mdomap[mdoch], flabel)

    def gainUUB(self, splitmode, uubnum, chan, flabel=""):
        """Return gain for UUB channel chan on UUB <uubnum>
uubnum - 1 .. 4000
chan - 1 .. 10
flabel - freq for sine wave or pulse if empty string"""
        index = self.uubnums.index(uubnum)
        group = SplitterGain.UUB2SPLIT[chan][0]
        return self._gain(splitmode, '%c%d' % (group, index), flabel)

    def _checksplitch(self, splitch):
        assert isinstance(splitch, str) and len(splitch) == 2
        assert splitch[0] in 'ABCDEF' and splitch[1] in '0123456789'

    def _gain(self, splitmode, splitch, flabel):
        """Return gain for splitter channel
splitch - i.e. C8"""
        if splitch == 'REF':
            return self.pregains[1]
        self._checksplitch(splitch)
        assert splitmode in (0, 1, 3)
        if splitmode == 0:
            gain = 1.0 / 32
        elif splitmode == 1 or splitch[0] != 'F':
            gain = 1.0
        else:
            gain = 4.0
        # splitter calibration as correction if available
        key = "%d%s%s" % (splitmode, splitch, flabel)
        correction = self.calibration.get(key, 1.0)
        # 0.5 hardcoded gain due impedance matching
        return 0.5 * self.pregains[0] * gain * correction


def DataProcessor(dp_ctx):
    """Data processor, a function to run in a separate process
dp_ctx - context with configuration (dict)
"""
    LOG_CONFIG = {
        'version': 1,
        'disable_existing_loggers': True,
        'handlers': {
            'queue': {
                'class': 'logging.handlers.QueueHandler',
                'queue': dp_ctx['q_log'],
            },
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['queue']
        },
    }
    logging.config.dictConfig(LOG_CONFIG)
    logger = logging.getLogger(multiprocessing.current_process().name)
    workhorses = [DP_store(dp_ctx['datadir']),
                  DP_ramp(dp_ctx['q_resp']),
                  DP_pede(dp_ctx['q_resp'])]
    if 'hswidth' in dp_ctx:
        workhorses.append(DP_hsampli(
            dp_ctx['q_resp'], dp_ctx['hswidth'],
            dp_ctx['lowgains'], dp_ctx['chans']))
        workhorses.append(DP_freq(
            dp_ctx['q_resp'], dp_ctx['lowgains'], dp_ctx['chans']))
    q_ndata = dp_ctx['q_ndata']
    logger.debug('init done')
    while True:
        try:
            nd = q_ndata.get()
        except SystemExit:
            break
        if nd is None:  # sentinel
            break
        logger.debug('converting UUB %04d, id %08x start',
                     nd.uubnum, nd.id)
        item = nd.details.copy() if nd.details is not None else {}
        item['uubnum'] = nd.uubnum
        item['yall'] = nd.convertData()
        label = item2label(item)
        logger.debug('conversion UUB %04d, id %08x done, processing %s',
                     nd.uubnum, nd.id, label)
        for wh in workhorses:
            try:
                wh.calculate(item)
            except Exception as e:
                logger.error('Workhorse %s with item = %s failed',
                             repr(wh), repr(item))
                logger.exception(e)
        logger.debug('Item %s done', label)
    logger.debug('finished')


LABEL_DOC = """ Label to item (and back) conversion
label = attr1_attr2 ... _attrn<functype>
attr are (optional, but in this order):
  attr          re in label   python in item       meaning
-------------------------------------------------------------------------------
  typ             [a-z]+      str            ampli, pede, pedesig ...
  timestamp       \d{14}      datetime       YYYYmmddHHMMSS (excl. tsmicro)
  timestampmicro  \d{14}      datetime       YYYYmmddHHMMSSffffff
  uubnum          u\d{4}      int 0-9999     u0015
  chan            c\d         int 1-10       c1 - c9, c0 .. channels 1 - 10
  splitch         s[A-F]\d    str            A-F .. 12, 34, 56, 7, 8, 9
  splitmode       a[013]      0, 1, 3        splitter mode
  voltage         v\d{2,3}    float          voltage coded as v1.v2v3 [volt]
  freq            f\d{2,4}    float          EM1M2M3 coded freq M1.M2M3*10^E Hz
  index           i\d{1,3}    int            index of measurement
  functype        [A-Z]       char           P - pulse series, F - sine,
                                             N - noise, R - ramp
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
    elif 'timestampmicro' in kwargs:
        attr.append(kwargs['timestampmicro'].strftime('%Y%m%d%H%M%S%f'))
    if 'uubnum' in kwargs:
        attr.append('u%04d' % kwargs['uubnum'])
    if 'chan' in kwargs:
        # transform chan 10 -> c0
        attr.append('c%d' % (kwargs['chan'] % 10))
    if 'splitch' in kwargs:
        arg = kwargs['splitch']
        assert isinstance(arg, str)
        assert arg == 'REF' or len(arg) == 2 and \
            arg[0] in 'ABCDEF' and arg[1] in '0123456789'
        attr.append('s' + arg)
    functype = kwargs.get('functype', '')
    if 'splitmode' in kwargs and functype in ('P', 'F'):
        assert kwargs['splitmode'] in (0, 1, 3)
        attr.append('a%d' % kwargs['splitmode'])
    if 'voltage' in kwargs and functype in ('P', 'F'):
        svolt = 'v%03d' % int(kwargs['voltage'] * 100.)
        if(svolt[-1] == '0'):
            svolt = svolt[:-1]
        attr.append(svolt)
    if functype == 'F':
        if 'flabel' in kwargs:
            attr.append('f' + kwargs['flabel'])
        elif 'freq' in kwargs:
            attr.append('f' + float2expo(kwargs['freq'], manlength=3))
    if 'index' in kwargs:
        attr.append('i%03d' % kwargs['index'])
    return '_'.join(attr) + functype


re_labels = [re.compile(regex) for regex in (
    r'(?P<typ>[a-z]+)$',
    r'(?P<timestamp>20\d{12})$',
    r'(?P<timestampmicro>20\d{18})$',
    r'u(?P<uubnum>\d{4})$',
    r'c(?P<chan>\d)$',
    r's(?P<splitch>[A-F]\d|REF)$',
    r'a(?P<splitmode>[013])$',
    r'v(?P<voltage>\d{2,3})$',
    r'f(?P<flabel>\d{2,4})$',
    r'i(?P<index>\d{3})$')]


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
    # uubnum, chan, splitmode and index to integers
    d.update({key: int(d[key])
              for key in ('uubnum', 'chan', 'splitmode', 'index') if key in d})
    if 'chan' in d and d['chan'] == 0:
        d['chan'] = 10
    # convert voltage back to float and chan 0 -> 10
    if 'voltage' in d:
        svolt = d['voltage']
        d['voltage'] = float('%c.%s' % (svolt[0], svolt[1:]))
    if 'flabel' in d:
        try:
            d['freq'] = expo2float(d['flabel'])
        except AssertionError:
            logging.getLogger('label2item').warning(
                'Wrong expo2float argument %s', d['freq'])
            del d['flabel']
    if 'timestamp' in d:
        try:
            d['timestamp'] = datetime.strptime(d['timestamp'], '%Y%m%d%H%M%S')
        except ValueError:
            logging.getLogger('label2item').warning(
                'Wrong timestamp %s (label %s)', d['timestamp'], label)
            del d['timestamp']
    if 'timestampmicro' in d:
        try:
            d['timestampmicro'] = datetime.strptime(d['timestampmicro'],
                                                    '%Y%m%d%H%M%S%f')
        except ValueError:
            logging.getLogger('label2item').warning(
                'Wrong timestampmicro %s (label %s)',
                d['timestampmicro'], label)
            del d['timestampmicro']
    return d


class DP_pede(object):
    """Data processor workhorse to calculate pedestals"""
    # parameters
    BINSTART = 0
    BINEND = 2047

    def __init__(self, q_resp):
        """Constructor.
q_resp - a logger queue
"""
        self.q_resp = q_resp
        logname = multiprocessing.current_process().name + '.pede'
        self.logger = logging.getLogger(logname)

    def calculate(self, item):
        if item.get('functype', None) != 'N':
            return
        self.logger.debug('Processing %s', item2label(item))
        array = item['yall'][self.BINSTART:self.BINEND, :]
        mean = array.mean(axis=0)
        stddev = array.std(axis=0)
        res = {'timestamp': item['timestamp']}
        itemr = {key: item[key]
                 for key in ('uubnum', 'functype', 'index')
                 if key in item}
        for ch, (m, s) in enumerate(zip(mean, stddev)):
            for typ, val in (('pede', m), ('pedesig', s)):
                label = item2label(itemr, typ=typ, chan=ch+1)
                res[label] = val
        self.q_resp.put(res)


class DP_hsampli(object):
    """Data processor workhorse to calculate amplitude of half-sines"""

    def __init__(self, q_resp, hswidth, lowgains, chans, **kwargs):
        """Constructor.
q_resp - a logger queue
hswidth - width of half-sine in microseconds
lowgains - UUB channels to process if splitmode == 1
chans - all UUB channels to process (all channels with signal)
kwargs: splitmode, voltage (fixed paramters)"""
        self.q_resp = q_resp
        logname = multiprocessing.current_process().name + '.hsampli'
        self.logger = logging.getLogger(logname)
        self.sf = SineFitter()
        self.lowgains, self.chans = lowgains, chans
        self.keys = {key: kwargs[key]
                     for key in ('splitmode', 'voltage')
                     if key in kwargs}
        self.q_resp = q_resp
        self.hsf = HalfSineFitter(hswidth)
        self.lowgains, self.chans = lowgains, chans

    def calculate(self, item):
        if item.get('functype', None) != 'P':
            return
        self.logger.debug('Processing %s', item2label(item))
        item = item.copy()
        item.update(self.keys)
        chans = self.lowgains if item['splitmode'] == 1 else self.chans
        yall = item['yall'][:, [chan-1 for chan in chans]]
        hsfres = self.hsf.fit(yall, HalfSineFitter.AMPLI)
        res = {'timestamp': item['timestamp']}
        itemr = {key: item[key]
                 for key in ('functype', 'uubnum', 'splitmode', 'voltage',
                             'index')
                 if key in item}
        itemr['typ'] = 'ampli'
        for chan, ampli in zip(chans, hsfres['ampli']):
            label = item2label(itemr, chan=chan)
            res[label] = ampli
        self.q_resp.put(res)


class DP_store(object):
    """Data processor workhorse to store 2048x10 data"""

    def __init__(self, datadir):
        """Constructor."""
        self.datadir = datadir
        logname = multiprocessing.current_process().name + '.store'
        self.logger = logging.getLogger(logname)

    def calculate(self, item):
        label = item2label(item)
        self.logger.debug('Processing %s', label)
        fn = '%s/dataall_%s.txt' % (self.datadir, label)
        np.savetxt(fn, item['yall'], fmt='% 5d')


class DP_freq(object):
    """Data processor workhorse to calculate amplitude of sines
for functype F"""

    def __init__(self, q_resp, lowgains, chans, **kwargs):
        """Constructor.
q_resp - a logger queue
lowgains - UUB channels to process when splitmode > 0
chans - UUB channels to process when splitmode = 0
        (all channels with signal)
kwargs: freq, splitmode, voltage (fixed paramters)"""
        self.q_resp = q_resp
        logname = multiprocessing.current_process().name + '.freq'
        self.logger = logging.getLogger(logname)
        self.sf = SineFitter()
        self.lowgains, self.chans = lowgains, chans
        self.keys = {key: kwargs[key]
                     for key in ('freq', 'splitmode', 'voltage')
                     if key in kwargs}

    def calculate(self, item):
        if item.get('functype', None) != 'F':
            return
        self.logger.debug('Processing %s', item2label(item))
        item = item.copy()
        item.update(self.keys)
        chans = self.lowgains if item['splitmode'] > 0 else self.chans
        yall = item['yall'][:, [chan-1 for chan in chans]]
        flabel = float2expo(item['freq'])
        sfres = self.sf.fit(yall, flabel, item['freq'], stage=SineFitter.AMPLI)
        res = {'timestamp': item['timestamp']}
        itemr = {key: item[key]
                 for key in ('functype', 'uubnum', 'flabel',
                             'freq', 'splitmode', 'voltage', 'index')
                 if key in item}
        itemr['typ'] = 'fampli'
        for chan, ampli in zip(chans, sfres['ampli']):
            label = item2label(itemr, chan=chan)
            res[label] = ampli
        self.q_resp.put(res)


class DP_ramp(object):
    """Data processor workhorse to checking test ramp"""

    def __init__(self, q_resp):
        """Constructor.
q_resp - a logger queue
"""
        self.q_resp = q_resp
        logname = multiprocessing.current_process().name + '.ramp'
        self.logger = logging.getLogger(logname)
        self.aramp = np.arange(2048, dtype='int16')

    def calculate(self, item):
        if item.get('functype', None) != 'R':
            return
        self.logger.debug('Processing %s', item2label(item))
        itemr = {key: item[key] for key in ('uubnum', 'functype')}
        res = {'timestamp': item['timestamp']}
        for ch in range(10):
            label = item2label(itemr, chan=ch+1)
            yr = np.array(item['yall'][:, ch], dtype='int16') + self.aramp
            yr %= 4096
            res[label] = np.amin(yr) == np.amax(yr)
        self.q_resp.put(res)


def make_DPfilter_stat(typ):
    """Dataprocessing filter
 - calculate staticstics on <typ>
input items:
   typ: <typ>, index, <others>
output items:
   typ: <typ>mean, <others>
   typ: <typ>stddev, <others>
others: uubnum, chan, splitmode, voltage, flabel (optional), functype
"""
    otherkeys = ('uubnum', 'chan', 'splitmode', 'voltage', 'flabel',
                 'functype')

    def filter_stat(res_in):
        data = {}
        for label, value in res_in.items():
            d = label2item(label)
            if d is None or 'typ' not in d or d['typ'] != typ:
                continue
            if 'freq' in d and 'flabel' not in d:
                d['flabel'] = float2expo(d['freq'])
            key = tuple([d.get(k, None) for k in otherkeys])
            if key not in data:
                data[key] = []
            data[key].append(value)
        res_out = res_in.copy()
        for key, valuelist in data.items():
            y = np.array(valuelist)
            item = {k: val for k, val in zip(otherkeys, key)
                    if val is not None}
            res_out[item2label(item, typ=typ+'mean')] = y.mean()
            res_out[item2label(item, typ=typ+'stddev')] = y.std()
        return res_out
    return filter_stat


def make_DPfilter_linear(lowgains, highgains, splitgain):
    """Dataprocessing filter
 - calculate linear fit & correlation coeff from ampli
input items:
   typ: ampli, functype: P + uubnum, chan, splitmode, voltage
or
   typ: fampli, functype: F + uubnum, chan, splitmode, voltage, freq
data: x - real voltage amplitude after splitter [mV],
      y - UUB ADCcount amplitude
output items:
    gain_u<uubnum>_c<uub channel>P - gain: ADC counts / mV
    lin_u<uubnum>_c<uub channel>P - linearity measure
or
    fgain_u<uubnum>_c<uub channel>_f<freq>F - gain: ADC counts / mV
    flin_u<uubnum>_c<uub channel>_f<freq>F  - linearity measure
"""
    keys = ('functype', 'uubnum', 'chan', 'flabel')
    outtypes = {'P': ('gain', 'lin'),
                'F': ('fgain', 'flin')}

    def filter_linear(res_in):
        data = {}
        for label, adcvalue in res_in.items():
            d = label2item(label)
            if d is None or 'typ' not in d or \
               d['typ'] not in ('ampli', 'fampli'):
                continue
            chan, splitmode, uubnum = d['chan'], d['splitmode'], d['uubnum']
            if not(chan in lowgains or chan in highgains and splitmode == 0):
                continue
            if d['typ'] == 'ampli':
                flabel = ''
                key = ('P', uubnum, chan)
            else:
                flabel = d.get('flabel', float2expo(d['freq']))
                key = ('F', uubnum, chan, flabel)
            gain = splitgain.gainUUB(splitmode, uubnum, chan, flabel)
            # voltage in mV
            volt = 1000*d['voltage'] * gain
            if key not in data:
                data[key] = []
            data[key].append((volt, adcvalue))
        res_out = res_in.copy()
        for key, xy in data.items():
            # xy = [[v1, adc1], [v2, adc2] ....]
            xy = np.array(xy)
            # xx_xy = [v1, v2, ...] * xy
            xx_xy = xy.T[0].dot(xy)
            slope = xx_xy[1] / xx_xy[0]
            covm = np.cov(xy, rowvar=False)
            # correlation coeff = cov(V, ADC) / sqrt(var(V) * var(ADC))
            if xy.shape[0] > 1:
                coeff = 1.0 - covm[0][1] / np.sqrt(covm[0][0] * covm[1][1])
            else:
                coeff = 1.0
            item = dict(zip(keys, key))
            for typ, value in zip(outtypes[key[0]], (slope, coeff)):
                label = item2label(item, typ=typ)
                res_out[label] = value
        return res_out
    return filter_linear


def make_DPfilter_cutoff():
    """Dataprocessing filter
 - calculate cut-off frequency from freqency gains
input items:
    fgain_u<uubnum>_c<uub channel>_f<freq> - frequency gain: ADC counts / mV
output items:
    cutoff_u<uubnum>_c<uub channel> - cut-off frequency [MHz]"""
    def filter_cutoff(res_in):
        data = {}
        for label, value in res_in.items():
            d = label2item(label)
            if d is None or 'typ' != 'fgain':
                continue
            key = (d['uubnum'], d['chan'], d['flabel'])
            data[key] = value
        # process data TBD
        nkeys = set([(key[0], key[1]) for key in data.keys()])
        res_out = {item2label(typ='cutoff', uubnum=key[0], chan=key[1]): 56.78
                   for key in nkeys}
        res_out['timestamp'] = res_in['timestamp']
        res_out['meas_point'] = res_in['meas_point']
        return res_out
    return filter_cutoff


def make_DPfilter_ramp(uubnums):
    """Dataprocessing filter
Check that for all UUBs and channels, all ramps are correct
log failed or missing labels.
res_out = {'timestamp', 'missing': <list>, 'failed': <list>}"""
    def aggregate(d, uubnum):
        "check if label present in <d> for all chans and replace them by one"
        labels = [item2label(functype='R', uubnum=uubnum, chan=ch+1)
                  for ch in range(10)]
        if all([label in d for label in labels]):
            for label in labels:
                d.remove(label)
            d.append(item2label(functype='R', uubnum=uubnum))

    def filter_ramp(res_in):
        data = {item2label(functype='R', uubnum=uubnum, chan=ch+1): None
                for uubnum in uubnums
                for ch in range(10)}
        for label, value in res_in.items():
            d = label2item(label)
            if d is None or 'functype' not in d or d['functype'] != 'R':
                continue
            data[label] = value
        missing = [label for label, value in data.items() if value is None]
        failed = [label for label, value in data.items() if value is False]
        # aggregate labels for UUB
        for uubnum in uubnums:
            aggregate(missing, uubnum)
            aggregate(failed, uubnum)
        res_out = {key: res_in[key]
                   for key in ('timestamp', 'meas_ramp', 'meas_point',
                               'db_ramp')
                   if key in res_in}
        if missing:
            res_out['ramp_missing'] = missing
        if failed:
            res_out['ramp_failed'] = failed
        return res_out
    return filter_ramp
