"""
  ESS procedure
  module for evaluation
"""

import sys
import threading
import logging
from datetime import datetime
from time import sleep

try:
    import zmq
except ImportError:
    zmq = None
from threadid import syscall, SYS_gettid
from UUB import VIRGINUUBNUM, uubnum2ip, isLive
from dataproc import item2label, expo2float

ZMQPORT = 5555


class EvalBase:
    """Base class for a particular evaluator"""
    # items of summary record
    ITEMS = ('pon', 'ramp', 'noise', 'pulse', 'freq', 'flir')
    PROLOGTEMPLATE = """\
# Evaluator log
# basetime %s
# evaluated UUBs: %s
# columns: eval type | meas_point | UUBnum | result | comment
"""
    LOGFMT = "{typ:5} {meas_point:4d} {uubnum:04d} {result:7s} {comment:s}\n"
    fplog = None

    def __init__(self, typ, uubnums, ctx=None):
        self.typ = typ
        self.label = 'Eval%s' % typ.capitalize()
        self.logger = logging.getLogger(self.label)
        self.uubnums = uubnums
        self.stats = None
        self.npoints = self.missing = 0
        self.lastmp = -1  # last meas point processed
        if EvalBase.fplog is None and ctx is not None:
            fn = ctx.datadir + ctx.basetime.strftime('eval-%Y%m%d.log')
            prolog = self.PROLOGTEMPLATE % (
                ctx.basetime.strftime('%Y-%m-%d %H:%M'),
                ', '.join(["%04d" % uubnum for uubnum in uubnums]))
            fplog = open(fn, 'a')
            fplog.write(prolog)
            EvalBase.fplog = fplog

    def summary(self, uubnum):
        """return JSON string result for particular UUB"""
        # default answer for base class
        if self.stats is None:
            return 'notapplicable'
        stat = self.stats[uubnum]
        if stat['failed'] > 0:
            return 'failed'
        if stat['ok'] >= self.npoints - self.missing:
            return 'passed'
        return 'error'

    def log(self, meas_point, uubnum, result, comment=''):
        if self.fplog is None:
            return
        msg = self.LOGFMT.format(
            typ=self.typ,
            meas_point=meas_point, uubnum=uubnum, result=result,
            comment=comment)
        self.fplog.write(msg)
        self.fplog.flush()

    def dpfilter(self, res_in):
        """DataProcessor filter implentation"""
        raise RuntimeError('Not implemented in base class')

    def stop(self):
        pass

    def configure_minmax(self, typ, kwargs):
        """Configure min/max intervals for quantity <typ>
kwargs[typ] - list of tuples (val_min, val_max, <channels>)
  value: float or None
  channels numbered from 1 to 10
  if value is None, test passes by default;
  missing channels passes by default"""
        assert typ in kwargs, '%s is mandatory' % typ
        limits = {chan: (None, None) for chan in range(1, 11)}
        for val_min, val_max, *chans in kwargs[typ]:
            assert isinstance(val_min, (type(None), int, float))
            assert isinstance(val_max, (type(None), int, float))
            assert all([chan in range(1, 11) for chan in chans])
            for chan in chans:
                limits[chan] = (val_min, val_max)
        self.limits[typ] = {chan: minmax for chan, minmax in limits.items()
                            if minmax != (None, None)}

    def check_minmax(self, res_in, typ, uubnum, failed, missing, comments,
                     flabel=None, freq=None):
        FUNCTYPE = {'noisemean': 'N',
                    'pedemean': 'N',
                    'pedestdev': 'N',
                    'gain': 'P',
                    'lin': 'P',
                    'hglgratio': 'P',
                    'cutoff': None}
        item = {'typ': typ, 'uubnum': uubnum}
        freqstr = ''
        if flabel is not None:
            item['functype'] = 'F'
            item['flabel'] = flabel
            freqstr = ' freq %.2fMHz' % (freq/1e6)
        elif FUNCTYPE[typ] is not None:
            item['functype'] = FUNCTYPE[typ]
        for chan, minmax in self.limits[typ].items():
            label = item2label(item, chan=chan)
            if label in res_in:
                val = res_in[label]
                if minmax[0] is not None and val < minmax[0]:
                    failed[chan] = True
                    comments.append('min %s @%s chan %d' % (
                        typ, freqstr, chan))
                if minmax[1] is not None and val > minmax[1]:
                    failed[chan] = True
                    comments.append('max %s @%s chan %d' % (
                        typ, freqstr, chan))
            else:
                missing[chan] = True
                comments.append('missing %s for%s chan %d' % (
                    typ, freqstr, chan))


class EvalRamp(EvalBase):
    """Eval ADC ramps"""
    # same as in make_DPfilter_ramp
    OK = 0
    MISSING = 0x4000
    FAILED = 0x2000

    def __init__(self, uubnums, **kwargs):
        ctx = kwargs.get('ctx', None)
        super(EvalRamp, self).__init__('ramp', uubnums, ctx=ctx)
        missing = kwargs.get('missing', None)
        self.missing = 2 if missing is None else int(missing)
        self.npoints = 0
        self.stats = {uubnum: {'ok': 0, 'missing': 0, 'failed': 0}
                      for uubnum in uubnums}
        self.logger.debug('creating instance with missing = %d', missing)

    def dpfilter(self, res_in):
        """Count ADC ramp results, expects DPfilter_ramp applied
return: does not modify res_in"""
        if 'meas_ramp' not in res_in:
            return res_in
        mp = res_in.get('meas_point', -1)
        if mp <= self.lastmp:  # avoid calling filter twice to one meas point
            self.logger.error('Duplicate call of dpfilter at measpoint %d', mp)
            return res_in
        self.lastmp = mp
        for uubnum in self.uubnums:
            label = item2label(typ='rampdb', uubnum=uubnum)
            rampres = res_in[label]
            stat = self.stats[uubnum]
            if rampres == self.OK:
                stat['ok'] += 1
            elif rampres == self.MISSING:
                stat['missing'] += 1
                self.log(mp, uubnum, 'missing')
            elif rampres & self.FAILED:
                stat['failed'] += 1
                self.log(mp, uubnum, 'failed', 'rampres %04x' % rampres)
            else:
                self.logger.error(
                    'Wrong ADC ramp result 0x%04x for uubnum %04d',
                    rampres, uubnum)
        self.npoints += 1
        return res_in


class EvalNoise(EvalBase):
    """Eval noise in ADC channels
missing - number of missing points to be still accepted as passed
noise - list of tuples (noise_min, noise_max, <channels>)
  if noise_min/max is None, test passed by default;
  missing channels passes by default
 - if any channel fails => UUB fails
 - elif any channel missing => missing point
 - else => passes
"""
    def __init__(self, uubnums, **kwargs):
        ctx = kwargs.get('ctx', None)
        super(EvalNoise, self).__init__('noise', uubnums, ctx=ctx)
        missing = kwargs.get('missing', None)
        self.missing = 2 if missing is None else int(missing)
        self.limits = {}
        for crit in ('noisemean', 'pedemean', 'pedestdev'):
            self.configure_minmax(crit, kwargs)
        self.npoints = 0
        self.stats = {uubnum: {'ok': 0, 'missing': 0, 'failed': 0}
                      for uubnum in uubnums}
        self.logger.debug('creating instance with missing = %d', missing)

    def dpfilter(self, res_in):
        """Count noise results, expects noise_stat filter applied
return: res_in + evalnoise_u<uubnum>_c<chan>N"""
        if 'meas_noise' not in res_in:
            return res_in
        mp = res_in.get('meas_point', -1)
        if mp <= self.lastmp:  # avoid calling filter twice to one meas point
            self.logger.error('Duplicate call of dpfilter at measpoint %d', mp)
            return res_in
        self.lastmp = mp
        res_out = res_in.copy()
        for uubnum in self.uubnums:
            failed = {chan: False for chan in range(1, 11)}
            missing = {chan: False for chan in range(1, 11)}
            comments = []
            for crit in self.limits.keys():
                self.check_minmax(res_in, crit, uubnum,
                                  failed, missing, comments)
            comment = ', '.join(comments) if comments else ''
            stat = self.stats[uubnum]  # shortcut
            if any(failed.values()):
                stat['failed'] += 1
                self.log(mp, uubnum, 'failed', comment)
            elif any(missing.values()):
                stat['missing'] += 1
                self.log(mp, uubnum, 'missing', comment)
            else:
                stat['ok'] += 1
            for chan in range(1, 11):
                label = item2label(typ='evalnoise', functype='N',
                                   uubnum=uubnum, chan=chan)
                if failed[chan]:
                    res_out[label] = False
                elif not missing[chan]:
                    res_out[label] = True
        self.npoints += 1
        return res_out


class EvalLinear(EvalBase):
    """Eval gain in ADC channels
missing - number of missing points to be still accepted as passed
mandatory paramters: gain, lin, hglgratio
 - if any channel fails => UUB fails
 - elif any channel missing => missing point
 - else => passes
"""
    def __init__(self, uubnums, **kwargs):
        ctx = kwargs.get('ctx', None)
        super(EvalLinear, self).__init__('pulse', uubnums, ctx=ctx)
        missing = kwargs.get('missing', None)
        self.missing = 2 if missing is None else int(missing)
        self.limits = {}
        for crit in ('gain', 'lin', 'hglgratio'):
            self.configure_minmax(crit, kwargs)
        self.npoints = 0
        self.stats = {uubnum: {'ok': 0, 'missing': 0, 'failed': 0}
                      for uubnum in uubnums}
        self.logger.debug('creating instance with missing = %d', missing)

    def dpfilter(self, res_in):
        """Count linear gain results, expects linear filter applied
return: res_in + evalpulse_u<uubnum>_c<chan>P"""
        if 'meas_pulse' not in res_in:
            return res_in
        mp = res_in.get('meas_point', -1)
        if mp <= self.lastmp:  # avoid calling filter twice to one meas point
            self.logger.error('Duplicate call of dpfilter at measpoint %d', mp)
            return res_in
        self.lastmp = mp
        res_out = res_in.copy()
        for uubnum in self.uubnums:
            failed = {chan: False for chan in range(1, 11)}
            missing = {chan: False for chan in range(1, 11)}
            comments = []
            for crit in self.limits.keys():
                self.check_minmax(res_in, crit, uubnum,
                                  failed, missing, comments)
            comment = ', '.join(comments) if comments else ''
            stat = self.stats[uubnum]  # shortcut
            if any(failed.values()):
                stat['failed'] += 1
                self.log(mp, uubnum, 'failed', comment)
            elif any(missing.values()):
                stat['missing'] += 1
                self.log(mp, uubnum, 'missing', comment)
            else:
                stat['ok'] += 1
            for chan in range(1, 11):
                label = item2label(typ='evalpulse', functype='P',
                                   uubnum=uubnum, chan=chan)
                if failed[chan]:
                    res_out[label] = False
                elif not missing[chan]:
                    res_out[label] = True
        self.npoints += 1
        return res_out


class EvalFreq(EvalBase):
    """Eval frequency gain and cut-off frequency in ADC channels"""
    def __init__(self, uubnums, **kwargs):
        ctx = kwargs.get('ctx', None)
        super(EvalFreq, self).__init__('freq', uubnums, ctx=ctx)
        missing = kwargs.get('missing', None)
        self.missing = 2 if missing is None else int(missing)
        self.flabels = {flabel: expo2float(flabel)
                        for flabel in kwargs['flabels']}
        self.limits = {}
        # frequency dependent decay
        self.freqdep = self.configure_freq('freqdep', kwargs)
        # flin: corr. coef. complement per frequency
        self.flin = self.configure_freq('flin', kwargs)
        # fgain_min/max(chan, flabel) = gain_min/max(chan) * freqdep(flabel)
        self.configure_minmax('gain', kwargs)
        self.configure_minmax('fhglgratio', kwargs)
        # cut-off frequency
        self.configure_minmax('cutoff', kwargs)
        self.npoints = 0
        self.stats = {uubnum: {'ok': 0, 'missing': 0, 'failed': 0}
                      for uubnum in uubnums}
        self.logger.debug('creating instance with missing = %d', missing)

    def configure_freq(self, typ, kwargs):
        """Expect dependence on frequency
kwargs[typ] - list of tuples (flabel, value)
return dict{flabel: value}"""
        assert typ in kwargs, 'freqdep is mandatory'
        d = dict(kwargs[typ])
        assert set(self.flabels.keys()) == set(d.keys())
        assert all([isinstance(val, (type(None), int, float))
                    for val in d.values()])
        return d

    def dpfilter(self, res_in):
        """Count frequency gain results, expects cut-off filter applied
return: res_in + evalfgain_u<uubnum>_c<chan>_f<flabel>F
               + evalcutoff_u<uubnum>_c<chan>F"""
        if 'meas_freq' not in res_in:
            return res_in
        mp = res_in.get('meas_point', -1)
        if mp <= self.lastmp:  # avoid calling filter twice to one meas point
            self.logger.error('Duplicate call of dpfilter at measpoint %d', mp)
            return res_in
        self.lastmp = mp
        res_out = res_in.copy()
        anyfailed = anymissing = False
        for uubnum in self.uubnums:
            comments = []
            for flabel, freq in self.flabels.items():
                failed = {chan: False for chan in range(1, 11)}
                missing = {chan: False for chan in range(1, 11)}
                # check fgain
                ffactor = self.freqdep[flabel]
                self.limits['fgain'] = {chan: [None, None]
                                        for chan in range(1, 11)}
                for chan, minmax in self.limits['gain'].items():
                    for i, val in enumerate(minmax):
                        if val is not None:
                            self.limits['fgain'][chan][i] = ffactor * val
                self.check_minmax(res_in, 'fgain', uubnum,
                                  failed, missing, comments, flabel, freq)
                # check HG/LG ratio
                self.check_minmax(res_in, 'fhglgratio', uubnum,
                                  failed, missing, comments, flabel, freq)
                # check flin
                linmax = self.flin[flabel]
                self.limits['flin'] = {chan: (None, linmax)
                                       for chan in range(1, 11)}
                self.check_minmax(res_in, 'flin', uubnum,
                                  failed, missing, comments, flabel, freq)
                for chan in range(1, 11):
                    label = item2label(typ='evalfgain', functype='F',
                                       uubnum=uubnum, flabel=flabel, chan=chan)
                    if failed[chan]:
                        res_out[label] = False
                    elif not missing[chan]:
                        res_out[label] = True
                if any(failed.values()):
                    anyfailed = True
                if any(missing.values()):
                    anymissing = True
            # check cut-off frequency
            failed = {chan: False for chan in range(1, 11)}
            missing = {chan: False for chan in range(1, 11)}
            self.check_minmax(res_in, 'cutoff', uubnum, failed, missing,
                              comments)
            for chan in range(1, 11):
                label = item2label(typ='evalcutoff', functype='F',
                                   uubnum=uubnum, chan=chan)
                if failed[chan]:
                    res_out[label] = False
                elif not missing[chan]:
                    res_out[label] = True
            if any(failed.values()):
                anyfailed = True
            if any(missing.values()):
                anymissing = True
            stat = self.stats[uubnum]  # shortcut
            comment = ', '.join(comments)
            if anyfailed:
                stat['failed'] += 1
                self.log(mp, uubnum, 'failed', comment)
            elif anymissing:
                stat['missing'] += 1
                self.log(mp, uubnum, 'missing', comment)
            else:
                stat['ok'] += 1
        self.npoints += 1
        return res_out


class EvalVoltramp(EvalBase):
    """Evaluate power on/off test with voltage ramp
<direction>_<state> - tuple (volt_min, volt_max);
    direction of voltage ramp: up/down; expected state after ramp: on/off
"""
    def __init__(self, uubnums, **kwargs):
        ctx = kwargs.get('ctx', None)
        super(EvalVoltramp, self).__init__('voltramp', uubnums, ctx=ctx)
        self.missing = 0
        limits = {}
        for direction in 'up', 'down':
            for state in 'on', 'off':
                key = direction + '_' + state
                if key in kwargs:
                    volt_min, volt_max = kwargs[key]
                    assert isinstance(volt_min, (type(None), int, float))
                    assert isinstance(volt_max, (type(None), int, float))
                    limits[direction+state] = (volt_min, volt_max)
        assert limits, 'No voltage ramp limits defined'
        self.limits = limits
        self.npoints = 0
        self.stats = {uubnum: {'ok': 0, 'missing': 0, 'failed': 0}
                      for uubnum in uubnums}
        self.logger.debug('creating instance')

    def dpfilter(self, res_in):
        """Count voltage ramp results
return: res_in + evalpon<vrtyp>_u<uubnum>"""
        if 'volt_ramp' not in res_in:
            return res_in
        mp = res_in.get('meas_point', -1)
        if mp <= self.lastmp:  # avoid calling filter twice to one meas point
            self.logger.error('Duplicate call of dpfilter at measpoint %d', mp)
            return res_in
        self.lastmp = mp
        res_out = res_in.copy()
        key = ''.join(res_in['volt_ramp'])
        labeltemplate = 'voltramp' + key + '_u%04d'
        volt_min, volt_max = self.limits[key]
        for uubnum in self.uubnums:
            passed = True
            stat = self.stats[uubnum]  # shortcut
            label = labeltemplate % uubnum
            if label in res_in:
                val = res_in[label]
                if volt_min is not None and val < volt_min:
                    passed = False
                    comment = 'volt_min'
                elif volt_max is not None and val > volt_max:
                    passed = False
                    comment = 'volt_max'
            else:
                passed = False
                comment = 'voltage missing'
            if passed:
                stat['ok'] += 1
            else:
                self.log(mp, uubnum, 'failed', comment)
                stat['failed'] += 1
            label = 'evalpon' + key + '_u%04d' % uubnum
            res_out[label] = passed
        self.npoints += 1
        return res_out


class EvalFLIR(EvalBase):
    """Evaluator for FLIR result"""
    def __init__(self, uubnums, **kwargs):
        ctx = kwargs.get('ctx', None)
        super(EvalFLIR, self).__init__('flir', uubnums, ctx=ctx)
        self.missing = 0
        self.stats = {uubnum: {'ok': 0, 'missing': 0, 'failed': 0}
                      for uubnum in uubnums}
        self.logger.debug('creating instance')

    def dpfilter(self, res_in):
        """Accumulate FLIR evaluation result
checks for keys: flireval_u%04d - True/False/None
return: does not modify res_in"""
        if 'meas_flir' not in res_in:
            return res_in
        mp = res_in.get('meas_point', -1)
        if mp <= self.lastmp:  # avoid calling filter twice to one meas point
            self.logger.error('Duplicate call of dpfilter at measpoint %d', mp)
            return res_in
        self.lastmp = mp
        for uubnum in self.uubnums:
            label = item2label(typ='flireval', uubnum=uubnum)
            if label in res_in:
                res = res_in[label]
                stat = self.stats[uubnum]
                if res is True:
                    stat['ok'] += 1
                elif res is False:
                    stat['failed'] += 1
                    self.log(mp, uubnum, 'failed')
                elif res is None:
                    stat['missing'] += 1
                    self.log(mp, uubnum, 'missing')
                else:
                    self.logger.error('wrong FLIR result ' + repr(res))
        self.npoints += 1
        return res_in


class Evaluator(threading.Thread):
    """Evaluator for check internal SN & removeUUB"""
    ISN_SEVERITY_STRICT = 0   # require all UUBs correctly
    ISN_SEVERITY_I2CFAIL = 1  # allow I2C between zynq and SC failure
    ISN_SEVERITY_NOTLIVE = 2  # allow UUB not live
    ISN_SEVERITY_NODB = 4     # allow ISN not in DB
    ISN_SEVERITY_REPORT = 8   # no action, just report status
    TOUT_ORD = 0.5            # timeout for UUB order check

    def __init__(self, ctx, fplist):
        """Constructor.
ctx - context object (i.e. ESS), used keys:
        timer
        uubnums
        internalSNs - dict
        uubtsc - uses uubtsc.internalSN
        critical_error - function to call to abort the test
fplist - list of files/streams for output
"""
        super(Evaluator, self).__init__(name='Thread-Evaluator')
        self.timer = ctx.timer
        self.uubnums = ctx.uubnums
        self.dbISN = ctx.internalSNs
        self.uubtsc = ctx.uubtsc
        self.pc = ctx.pc
        self.critical_error = ctx.critical_error
        self.removeUUB = ctx.removeUUB
        self.fplist = fplist
        self.thrs = []  # threads removing UUB to join
        if zmq is not None:
            self.zmqcontext = zmq.Context()
            self.zmqsocket = self.zmqcontext.socket(zmq.PUB)
            self.zmqsocket.bind("tcp://127.0.0.1:%d" % ZMQPORT)
        self.logger = logging.getLogger('Evaluator')
        self._lock_msg = threading.Lock()

    def run(self):
        tid = syscall(SYS_gettid)
        self.logger.debug('run start, name %s, tid %d',
                          threading.current_thread().name, tid)
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                self.logger.info('Evaluator stopped')
                return
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags

            # join removeUUB threads
            if self.thrs:
                thrs = []  # new list for unjoined threads
                for thr in self.thrs:
                    thr.join(0.001)
                    if thr.is_alive():
                        self.logger.warning('Join %s timeouted', thr.name)
                        thrs.append(thr)
                self.thrs = thrs

            if 'eval' in flags:
                flags = flags['eval']
            else:
                continue

            if 'checkISN' in flags:
                self.checkISN(flags['checkISN'], timestamp)

            if 'orderUUB' in flags:
                thr = threading.Thread(
                    target=self.orderUUB, name='Thread-orderUUB',
                    args=(flags['orderUUB'], timestamp))
                self.thrs.append(thr)
                thr.start()

            if 'removeUUB' in flags:
                for uubnum in flags['removeUUB']:
                    thr = threading.Thread(
                        target=self.removeUUB, name='Thread-removeUUB',
                        args=(uubnum, self.logger))
                    thr.start()
                    self.thrs.append(thr)

            if 'message' in flags:
                msglines = flags['message'].splitlines()
                self.writeMsg(msglines, timestamp)

    def checkISN(self, isn_severity=None, timestamp=None):
        """Check internal SN and eventually call ess.critical_error"""
        self.logger.info('Checking internal SN')
        if isn_severity is None:
            isn_severity = Evaluator.ISN_SEVERITY_STRICT  # default value
        testres = True
        luubnums = [uubnum for uubnum in self.uubnums
                    if uubnum is not None]
        uubISN = {uubnum: self.uubtsc[uubnum].internalSN
                  for uubnum in luubnums}
        zVirgin = VIRGINUUBNUM in luubnums
        if zVirgin:
            assert len(luubnums) == 2, \
                "must be just one UUB together with VIRGIN UUB"

        nodb = [uubnum for uubnum in luubnums
                if uubnum not in self.dbISN and uubnum != VIRGINUUBNUM]
        if nodb:
            self.logger.info('UUBs not found in DB: %s',
                             ', '.join(['%04d' % uubnum for uubnum in nodb]))
            if isn_severity & Evaluator.ISN_SEVERITY_NODB == 0:
                testres = False
        else:
            self.logger.info('All UUBs found in DB')

        i2cfail = [uubnum for uubnum in luubnums
                   if uubISN[uubnum] is False]
        if i2cfail:
            self.logger.info(
                'UUBs that failed to read ISN: %s',
                ', '.join(['%04d' % uubnum for uubnum in i2cfail]))
            if isn_severity & Evaluator.ISN_SEVERITY_I2CFAIL == 0:
                testres = False

        notlive = [uubnum for uubnum in luubnums
                   if uubISN[uubnum] is None]
        if zVirgin:
            virginLive = None
            if len(notlive) == 0:
                self.logger.error('Seems both UUB and virgin live')
            elif len(notlive) == 2:
                self.logger.warning('UUB not live')
                if isn_severity & Evaluator.ISN_SEVERITY_NOTLIVE == 0:
                    testres = False
            else:
                virginLive = VIRGINUUBNUM not in notlive
                if not nodb and not i2cfail:
                    uubnum, disn = list(self.dbISN.items())[0]
                    uisn = uubISN[VIRGINUUBNUM if virginLive else uubnum]
                    if disn != uisn:
                        testres = False
                        self.logger.error(
                            'ISN mismatch for UUB #%04d, DB %s vs UUB %s',
                            uubnum, disn, uisn)
        else:
            if notlive:
                self.logger.info(
                    'UUBs still not live: %s',
                    ', '.join(['%04d' % uubnum for uubnum in notlive]))
                if isn_severity & Evaluator.ISN_SEVERITY_NOTLIVE == 0:
                    testres = False
            invalid = [(uubnum, self.dbISN[uubnum], uubISN[uubnum])
                       for uubnum in luubnums
                       if uubnum not in nodb + i2cfail + notlive and
                       self.dbISN[uubnum] != uubISN[uubnum]]
            if invalid:
                testres = False
                for uubnum, disn, uisn in invalid:
                    self.logger.error(
                        'ISN mismatch for UUB #%04d, DB %s vs UUB %s',
                        uubnum, disn, uisn)

        msglines = ['Check of internal serial number(s) %s.'
                    % ('passed' if testres else 'failed')]
        if zVirgin and virginLive is not None:
            msglines.append('UUB running under %s MAC address.'
                            % ('original' if virginLive else 'changed'))
        zAbort = not testres and (
            isn_severity & Evaluator.ISN_SEVERITY_REPORT == 0)
        if zAbort:
            msglines.append('The test will be aborted now.')
        self.writeMsg(msglines, timestamp)
        self.logger.info(' '.join(msglines))
        if zAbort:
            self.critical_error()

    def orderUUB(self, abort=True, timestamp=None):
        """Find order of UUBs connected to powercontrol.
Suppose all UUBs are booted, switch them one by one to determine their order
Return their order
Raise AssertionError in a non-allowed situation"""
        tid = syscall(SYS_gettid)
        self.logger.debug('Checkin UUB order, name %s, tid %d',
                          threading.current_thread().name, tid)
        uubset_all = {uubnum for uubnum in self.uubnums if uubnum is not None}
        maxind = max([i for i, uubnum in enumerate(self.uubnums)
                      if uubnum is not None])
        uub2ip = {uubnum: uubnum2ip(uubnum) for uubnum in uubset_all}
        uubset_exp = {uubnum for uubnum in uubset_all
                      if isLive(uub2ip[uubnum], self.logger)}
        uubnums = []  # tested order of UUBs
        portmask = 1  # raw ports to switch off
        for n in range(9, -1, -1):  # expected max number of live UUBs
            self.pc.switchRaw(False, portmask)
            portmask <<= 1
            sleep(Evaluator.TOUT_ORD)
            uubset_real = {uubnum for uubnum in uubset_all
                           if isLive(uub2ip[uubnum], self.logger)}
            self.logger.debug(
                'n = %d, UUBs still live = %s', n,
                ', '.join(['%04d' % uubnum for uubnum in uubset_real]))
            assert(len(uubset_real) <= n), 'Too much UUBs still live'
            assert(uubset_real <= uubset_exp), 'UUB reincarnation?'
            diflist = list(uubset_exp - uubset_real)
            assert len(diflist) <= 1, 'More than 1 UUB died'
            uubnums.append(diflist[0] if diflist else None)
            uubset_exp = uubset_real

        maxind = max([maxind] + [i for i, uubnum in enumerate(self.uubnums)
                                 if uubnum is not None])
        zFail = uubnums[:maxind+1] != self.uubnums[:maxind+1]
        if zFail:
            uubs = ['%04d' % uubnum if uubnum else 'null'
                    for uubnum in uubnums]
            msglines = ['Incorrect UUB numbers.',
                        'Detected UUBs: [ %s ].' % ', '.join(uubs)]
            if abort:
                msglines.append('Aborting.')
            self.writeMsg(msglines, timestamp)
            self.logger.info(' '.join(msglines))
        if abort and zFail:
            self.critical_error()

        return uubnums

    def writeMsg(self, msglines, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now()
        ts = timestamp.strftime('%Y-%m-%dT%H:%M:%S | ')
        spacer = ' ' * len(ts)
        with self._lock_msg:
            for line in msglines:
                msg = ts + line + '\n'
                ts = spacer
                for fp in self.fplist:
                    fp.write(msg)
                if zmq is not None:
                    self.zmqsocket.send_string(msg)

    def join(self, timeout=None):
        while self.thrs:
            try:
                thr = self.thrs.pop()
            except IndexError:
                break
            thr.join()
        super(Evaluator, self).join(timeout)

    def stopZMQ(self):
        if zmq is not None and self.zmqsocket is not None:
            self.zmqsocket.close()
            self.zmqsocket = None


def msg_client():
    """Listen for messages distributed by ZMQ"""
    assert zmq is not None, "ZMQ not imported"
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://127.0.0.1:%d" % ZMQPORT)
    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    while True:
        try:
            sys.stdout.write(socket.recv_string())
        except KeyboardInterrupt:
            break
