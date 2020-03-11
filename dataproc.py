"""
 ESS procedure
 data processor
"""

import re
import multiprocessing
import logging
import json
import math
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


def freqlabel(item):
    """Extract flabel and freq from dict item
return (flabel, freq) or raise ValueError"""
    if 'flabel' in item:
        flabel = item['flabel']
        if 'freq' in item:
            freq = item['freq']
        else:
            freq = expo2float(flabel)
    elif 'freq' in item:
        freq = item['freq']
        flabel = float2expo(freq)
    else:
        raise ValueError('Neither flabel nor freq present in %s', repr(item))
    return flabel, freq


class DirectGain(object):
    """Gain if not splitter in chain"""
    def gainUUB(self, splitmode, uubnum, chan, flabel=""):
        return 1.0


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
    SPLITGAIN = 0.5  # hardcoded splitter gain due impedance matching

    def __init__(self, pregains=(1, None), mdochans=None, uubnums=None,
                 calibration=None):
        """Constructor.
pregain - afg.gains for splitter input (float)
mdochans - list of upto 4 splitter channels
uubnums - list of upto 10 UUB numbers
calibration - dict(key: correction_value),
              key: "%d%s%s" % (splitmode, splitch, flabel)
              correction_value: 1.0 in ideal case
channels: [A-F][0-9] ... splitter, on AFG chan A
          R[0-9] ... reference, on AFG chan B
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
                                   splitch[0] in 'ABCDEFR' and
                                   splitch[1] in '0123456789')}
            if any([splitch[0] in 'ABCDEF'
                    for splitch in self.mdomap.values()]):
                assert self.pregains[0] is not None
            if any([splitch[0] == 'R' for splitch in self.mdomap.values()]):
                assert self.pregains[1] is not None
        self.uubnums = uubnums if uubnums is not None else [None]
        if calibration is not None:
            self.calibration = json.load(open(calibration, 'r'))
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

    @staticmethod
    def _checksplitch(splitch):
        assert isinstance(splitch, str) and len(splitch) == 2
        assert splitch[0] in 'ABCDEFR' and splitch[1] in '0123456789'

    def _gain(self, splitmode, splitch, flabel):
        """Return gain for splitter channel
splitch - i.e. C8"""
        SplitterGain._checksplitch(splitch)
        if splitch[0] == 'R':
            return self.pregains[1]
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
        return self.SPLITGAIN * self.pregains[0] * gain * correction


def make_notcalc(ctx, pedestals=[250.0]*10):
    """Factory to create the notcalc function
ctx - dict with default Pvoltage and Fvoltage
pedestals - positions of pedestals [ADC]
"""
    Pvoltage = ctx.get('Pvoltage', None)
    Fvoltage = ctx.get('Fvoltage', None)
    gainLG, gainHG = 2.0e3, 64.0e3   # ADC/V
    margin = 3900  # maximal ADC
    maxvolt = {}  # maxv[splitmode][chan]
    maxvolt[None] = [None] + [(margin - pedestals[chan-1])/(
        gainHG if SplitterGain.UUB2SPLIT[chan][1] else gainLG)
                              for chan in range(1, 11)]
    maxvolt[None][9] *= 4  # low-low gain
    maxvolt[1] = maxvolt[None]
    maxvolt[0] = [None] + [v * 32 for v in maxvolt[1][1:]]
    maxvolt[3] = maxvolt[None][:]
    maxvolt[3][9] /= 4
    maxvolt[3][10] /= 4

    def notcalc(functype, chan, splitmode=None, voltage=None):
        assert splitmode in (None, 0, 1, 3), "Wrong splitmode"
        assert chan in range(1, 11)
        assert functype in ('P', 'F')
        if functype == 'F':
            if voltage is None:
                voltage = 2*Fvoltage
            else:
                voltage *= 2
        else:
            if voltage is None:
                voltage = Pvoltage
        return voltage > maxvolt[splitmode][chan]
    return notcalc


def DataProcessor(dp_ctx):
    """Data processor, a function to run in a separate process
dp_ctx - context with configuration (dict)
"""
    MINFTY = datetime(2016, 1, 1)  # minus infinity
    CHS = range(10)  # all chans -1
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
    temp_invalid_chs = []  # for DP_ramp to report failed chs
    workhorses = [DP_store(dp_ctx['datadir']),
                  DP_ramp(dp_ctx['q_resp'], temp_invalid_chs),
                  DP_pede(dp_ctx['q_resp'])]
    if 'hswidth' in dp_ctx:
        notcalc = make_notcalc(dp_ctx)
        workhorses.append(DP_hsampli(
            dp_ctx['q_resp'], dp_ctx['hswidth'], notcalc, dp_ctx['chans'],
            splitmode=dp_ctx['splitmode'], voltage=dp_ctx['Pvoltage']))
        workhorses.append(DP_freq(
            dp_ctx['q_resp'], notcalc, dp_ctx['chans'],
            splitmode=dp_ctx['splitmode'], voltage=dp_ctx['Fvoltage'],
            freq=dp_ctx['freq']))
    q_ndata = dp_ctx['q_ndata']
    # shared dict with invalid channels per (ts, uubnum)
    invalid_chs_dict = dp_ctx['inv_chs_dict']
    chs = {}   # valid channels
    last_ts = MINFTY
    logger.debug('init done')
    while True:
        try:
            nd = q_ndata.get()
        except SystemExit:
            break
        if nd is None:  # sentinel
            q_ndata.task_done()
            break
        logger.debug('converting UUB %04d, id %08x start',
                     nd.uubnum, nd.id)
        item = nd.details.copy() if nd.details is not None else {}
        uubnum = nd.uubnum
        timestamp = item.get('timestamp', MINFTY)
        if item.get('functype', 'R') != 'R':
            # only set chs if functype defined and not ramp
            if timestamp > last_ts:  # new measurement point
                chs = {}
                last_ts = timestamp
            if uubnum not in chs:  # retrieve from ess
                ichs = invalid_chs_dict.get((timestamp, uubnum), None)
                if ichs is None:
                    chs[uubnum] = None
                else:
                    chs[uubnum] = [ch for ch in CHS if ch not in ichs]
            if chs[uubnum] is not None:
                item['chs'] = chs[uubnum]
        item['uubnum'] = uubnum
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
        if temp_invalid_chs:
            logger.debug('reporting invalid channels (%s, %04d): %s',
                         timestamp.strftime('%Y-%m-%d %H:%M:%S'), uubnum,
                         repr(temp_invalid_chs))
            invalid_chs_dict[(timestamp, uubnum)] = temp_invalid_chs
            del temp_invalid_chs[:]  # clear so DP_ramp may set failed chs
        logger.debug('Item %s done', label)
        q_ndata.task_done()
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
  splitch         s[A-F]\d    str            A-F .. 12, 34, 56, 7, 8, 9&10
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
        SplitterGain._checksplitch(arg)
        attr.append('s' + arg)
    functype = kwargs.get('functype', '')
    if 'splitmode' in kwargs and functype in ('P', 'F'):
        assert kwargs['splitmode'] in (0, 1, 3)
        attr.append('a%d' % kwargs['splitmode'])
    if 'voltage' in kwargs and functype in ('P', 'F'):
        if functype == 'F':
            svolt = 'v%03d' % int(kwargs['voltage'] * 200.)
        else:
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
    r's(?P<splitch>[A-FR]\d)$',
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
    # convert voltage back to float
    if 'voltage' in d:
        svolt = d['voltage']
        d['voltage'] = float('%c.%s' % (svolt[0], svolt[1:]))
        if d['functype'] == 'F':
            d['voltage'] /= 2
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
    CHS = tuple(range(10))

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
        chs = item.get('chs', self.CHS)
        array = item['yall'][self.BINSTART:self.BINEND, chs]
        mean = array.mean(axis=0)
        stdev = array.std(axis=0, ddof=1)
        res = {'timestamp': item['timestamp']}
        itemr = {key: item[key]
                 for key in ('uubnum', 'functype', 'index')
                 if key in item}
        for ch, m, s in zip(chs, mean, stdev):
            for typ, val in (('pede', m), ('noise', s)):
                label = item2label(itemr, typ=typ, chan=ch+1)
                res[label] = val
        self.q_resp.put(res)


class DP_hsampli(object):
    """Data processor workhorse to calculate amplitude of half-sines"""

    def __init__(self, q_resp, hswidth, notcalc, chans, **kwargs):
        """Constructor.
q_resp - a logger queue
hswidth - width of half-sine in microseconds
notcalc - function of functype, chan, splitmode, voltage
          to return True if overflow expected (channel not processed)
chans - all UUB channels to process (all channels with signal)
kwargs: splitmode, voltage (fixed paramters)"""
        self.q_resp = q_resp
        logname = multiprocessing.current_process().name + '.hsampli'
        self.logger = logging.getLogger(logname)
        self.sf = SineFitter()
        self.notcalc = notcalc
        self.chs = [chan-1 for chan in chans]
        self.keys = {key: kwargs[key]
                     for key in ('splitmode', 'voltage')
                     if key in kwargs}
        self.q_resp = q_resp
        self.hsf = HalfSineFitter(hswidth)

    def calculate(self, item):
        if item.get('functype', None) != 'P':
            return
        self.logger.debug('Processing %s', item2label(item))
        splitmode = item.get('splitmode', self.keys['splitmode'])
        voltage = item.get('voltage', self.keys['voltage'])
        chs = [ch for ch in item.get('chs', self.chs)
               if not self.notcalc('P', ch+1, splitmode, voltage)]
        yall = item['yall'][:, chs]
        hsfres = self.hsf.fit(yall, HalfSineFitter.AMPLI)
        res = {'timestamp': item['timestamp']}
        itemr = {key: item[key]
                 for key in ('functype', 'uubnum', 'splitmode', 'voltage',
                             'index')
                 if key in item}
        itemr['typ'] = 'ampli'
        for ch, ampli in zip(chs, hsfres['ampli']):
            label = item2label(itemr, chan=ch+1)
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

    def __init__(self, q_resp, notcalc, chans, **kwargs):
        """Constructor.
q_resp - a logger queue
notcalc - function of functype, chan, splitmode, voltage
          to return True if overflow expected (channel not processed)
chans - UUB channels to process when splitmode = 0
        (all channels with signal)
kwargs: freq, splitmode, voltage (fixed paramters)"""
        self.q_resp = q_resp
        logname = multiprocessing.current_process().name + '.freq'
        self.logger = logging.getLogger(logname)
        self.sf = SineFitter()
        self.notcalc = notcalc
        self.chs = [chan-1 for chan in chans]
        self.keys = {key: kwargs[key]
                     for key in ('splitmode', 'voltage', 'flabel', 'freq')
                     if key in kwargs}

    def calculate(self, item):
        if item.get('functype', None) != 'F':
            return
        self.logger.debug('Processing %s', item2label(item))
        splitmode = item.get('splitmode', self.keys['splitmode'])
        voltage = item.get('voltage', self.keys['voltage'])
        try:
            flabel, freq = freqlabel(item)
        except ValueError:
            flabel, freq = freqlabel(self.keys)
        chs = [ch for ch in item.get('chs', self.chs)
               if not self.notcalc('F', ch+1, splitmode, voltage)]
        yall = item['yall'][:, chs]
        sfres = self.sf.fit(yall, flabel, freq, stage=SineFitter.AMPLI)
        res = {'timestamp': item['timestamp']}
        itemr = {key: item[key]
                 for key in ('functype', 'uubnum', 'flabel',
                             'freq', 'splitmode', 'voltage', 'index')
                 if key in item}
        itemr['typ'] = 'fampli'
        for ch, ampli in zip(chs, sfres['ampli']):
            label = item2label(itemr, chan=ch+1)
            res[label] = ampli
        self.q_resp.put(res)


class DP_ramp(object):
    """Data processor workhorse to checking test ramp"""
    CHS = tuple(range(10))

    def __init__(self, q_resp, invalid_chs=None):
        """Constructor.
q_resp - a logger queue
invalid_chs - if not None, store there list of failed channels (-1)
"""
        self.q_resp = q_resp
        self.invalid_chs = invalid_chs
        logname = multiprocessing.current_process().name + '.ramp'
        self.logger = logging.getLogger(logname)
        self.aramp = np.arange(2048, dtype='int16')

    def calculate(self, item):
        if item.get('functype', None) != 'R':
            return
        self.logger.debug('Processing %s', item2label(item))
        itemr = {key: item[key] for key in ('uubnum', 'functype')}
        res = {'timestamp': item['timestamp']}
        for ch in self.CHS:
            label = item2label(itemr, chan=ch+1)
            yr = np.array(item['yall'][:, ch], dtype='int16') + self.aramp
            yr %= 4096
            rampOK = bool(np.amin(yr) == np.amax(yr))
            if self.invalid_chs is not None and not rampOK:
                self.invalid_chs.append(ch)
            res[label] = rampOK
        self.q_resp.put(res)


def make_DPfilter_stat(typ):
    """Dataprocessing filter
 - calculate staticstics on <typ>
input items:
   typ: <typ>, index, <others>
output items (stdev=standard deviation estimation):
   typ: <typ>mean, <others>
   typ: <typ>stdev, <others>
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
            res_out[item2label(item, typ=typ+'stdev')] = y.std(ddof=1)
        return res_out
    return filter_stat


def make_DPfilter_linear(notcalc, splitgain):
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
            chan, uubnum, voltage = d['chan'], d['uubnum'], d['voltage']
            splitmode = d.get('splitmode', None)
            if d['typ'] == 'ampli':
                flabel = ''
                key = ('P', uubnum, chan)
            else:
                flabel = d.get('flabel', float2expo(d['freq']))
                key = ('F', uubnum, chan, flabel)
            if notcalc(key[0], chan, splitmode, voltage):
                continue
            gain = splitgain.gainUUB(splitmode, uubnum, chan, flabel)
            # voltage in mV
            volt = 1000*voltage * gain
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
    label_ref = '71'  # 10 MHz
    label_line = ('75', '759', '77')  # 50, 59 and 70 MHz
    freq_scale = 60.e6                # frequency to scale other frequencies
    X = np.ones((len(label_line), 2))
    X[:, 0] = [expo2float(lab) / freq_scale for lab in label_line]
    M = np.matmul(np.linalg.inv(np.matmul(X.T, X)), X.T)

    def filter_cutoff(res_in):
        data = {}
        for label, value in res_in.items():
            d = label2item(label)
            if d is None or d.get('typ', None) != 'fgain':
                continue
            key = (d['uubnum'], d['chan'])
            if key not in data:
                data[key] = {d['flabel']: value}
            else:
                data[key][d['flabel']] = value
        res_out = res_in.copy()
        for key, fd in data.items():
            cutoff_val = fd[label_ref] / math.sqrt(2.)
            y = np.array([fd[lab] for lab in label_line])
            a, b = np.matmul(M, y)
            freq_co = (cutoff_val - b)/a * freq_scale / 1.0e6
            label = item2label(typ='cutoff', uubnum=key[0], chan=key[1])
            res_out[label] = freq_co
        return res_out
    return filter_cutoff


def make_DPfilter_ramp(uubnums):
    """Dataprocessing filter
Check that for all UUBs and channels, all ramps are correct
log failed or missing labels.
res_out = {'timestamp', 'missing': <list>, 'failed': <list>}"""
    OK = 0
    MISSING = 0x4000
    FAILED = 0x2000

    def aggregate(d, uubnum):
        "check if label present in <d> for all chans and replace them by one"
        labels = [item2label(functype='R', uubnum=uubnum, chan=ch+1)
                  for ch in range(10)]
        if all([label in d for label in labels]):
            for label in labels:
                d.remove(label)
            d.append(item2label(functype='R', uubnum=uubnum))
        bitmap = sum([1 << ch for ch, label in enumerate(labels)
                      if label in d])
        return bitmap

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
        res_out = {key: res_in[key]
                   for key in ('timestamp', 'meas_ramp', 'meas_point',
                               'db_ramp')
                   if key in res_in}
        # aggregate labels for UUB
        for uubnum in uubnums:
            label = item2label(typ='rampdb', uubnum=uubnum)
            bitmap_m = aggregate(missing, uubnum)
            bitmap_f = aggregate(failed, uubnum)
            if bitmap_m > 0:  # should be 2**10 - 1
                res_out[label] = MISSING
            elif bitmap_f > 0:
                res_out[label] = FAILED | bitmap_f
            else:
                res_out[label] = OK
        if missing:
            res_out['ramp_missing'] = missing
        if failed:
            res_out['ramp_failed'] = failed
        return res_out
    return filter_ramp
