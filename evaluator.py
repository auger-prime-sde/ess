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
from dataproc import item2label

ZMQPORT = 5555


class EvalBase(object):
    """Base class for a particular evaluator"""
    # items of summary record
    ITEMS = ('pon', 'ramp', 'noise', 'pulse', 'freq', 'flir')

    def __init__(self, typ, uubnums):
        self.typ = typ
        self.label = 'Eval%s' % typ.capitalize()
        self.logger = logging.getLogger(self.label)
        self.uubnums = uubnums

    def summary(self, uubnum):
        """return JSON string result for particular UUB"""
        # default answer for base class
        return 'notapplicable'

    def write_rec(self, d):
        """LogHandler.write_rec implentation"""
        raise RuntimeError('Not implemented in base class')

    def stop(self):
        pass


class EvalRamp(EvalBase):
    """Eval ADC ramps"""
    # same as in make_DPfilter_ramp
    OK = 0
    MISSING = 0x4000
    FAILED = 0x2000

    def __init__(self, uubnums, **kwargs):
        super(EvalRamp, self).__init__('ramp', uubnums)
        missing = kwargs.get('missing', None)
        self.missing = 2 if missing is None else int(missing)
        self.npoints = 0
        self.stats = {uubnum: {'ok': 0, 'missing': 0, 'failed': 0}
                      for uubnum in uubnums}
        self.logger.debug('creating instance with missing = %d', missing)

    def write_rec(self, d):
        """Count ADC ramp results, expects DPfilter_ramp applied"""
        if 'meas_ramp' not in d:
            return
        for uubnum in self.uubnums:
            label = item2label(typ='rampdb', uubnum=uubnum)
            rampres = d[label]
            stat = self.stats[uubnum]
            if rampres == self.OK:
                stat['ok'] += 1
            elif rampres == self.MISSING:
                stat['missing'] += 1
            elif rampres & self.FAILED:
                stat['failed'] += 1
            else:
                self.logger.error(
                    'Wrong ADC ramp result 0x%04x for uubnum %04d',
                    rampres, uubnum)
        self.npoints += 1

    def summary(self, uubnum):
        stat = self.stats[uubnum]
        if stat['failed'] > 0:
            return 'failed'
        elif stat['ok'] >= self.npoints - self.missing:
            return 'passed'
        else:
            return 'error'


class EvalNoise(EvalBase):
    """Eval noise in ADC channels
missing - number of missing points to be still accepted as passed
chanoise - list of tuples (noise_min, noise_max, <channels>)
  if noise_min/max is None, test passed by default;
  missing channels passes by default
 - if any channel fails => UUB fails
 - elif any channel missing => missing point
 - else => passes
"""
    def __init__(self, uubnums, **kwargs):
        super(EvalNoise, self).__init__('noise', uubnums)
        missing = kwargs.get('missing', None)
        self.missing = 2 if missing is None else int(missing)
        assert 'chanoise' in kwargs, 'chanoise is mandatory'
        limits = {chan: (None, None) for chan in range(1, 11)}
        for noise_min, noise_max, *chans in kwargs['chanoise']:
            assert isinstance(noise_min, (type(None), int, float))
            assert isinstance(noise_max, (type(None), int, float))
            assert all([chan in range(1, 11) for chan in chans])
            for chan in chans:
                limits[chan] = (noise_min, noise_max)
        # remove None, None limits
        self.limits = {chan: minmax for chan, minmax in limits.items()
                       if minmax != (None, None)}
        self.npoints = 0
        self.stats = {uubnum: {'ok': 0, 'missing': 0, 'failed': 0}
                      for uubnum in uubnums}
        self.logger.debug('creating instance with missing = %d', missing)

    def write_rec(self, d):
        """Count noise results, expects noise_stat filter applied"""
        if 'meas_noise' not in d:
            return
        for uubnum in self.uubnums:
            item = {'functype': 'N', 'typ': 'noisemean', 'uubnum': uubnum}
            passed = True
            missing = False
            for chan, minmax in self.limits.items():
                label = item2label(item, chan=chan)
                if label in d:
                    val = d[label]
                    if minmax[0] is not None:
                        passed &= minmax[0] <= val
                    if minmax[1] is not None:
                        passed &= val <= minmax[1]
                    if not passed:  # skip rest of for(chan, minmax)
                        break
                else:
                    missing = True
            stat = self.stats[uubnum]  # shortcut
            if not passed:
                stat['failed'] += 1
            elif missing:
                stat['missing'] += 1
            else:
                stat['ok'] += 1
        self.npoints += 1

    def summary(self, uubnum):
        stat = self.stats[uubnum]
        if stat['failed'] > 0:
            return 'failed'
        elif stat['ok'] >= self.npoints - self.missing:
            return 'passed'
        else:
            return 'error'


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
        uubset_all = set([uubnum for uubnum in self.uubnums
                          if uubnum is not None])
        maxind = max([i for i, uubnum in enumerate(self.uubnums)
                      if uubnum is not None])
        uub2ip = {uubnum: uubnum2ip(uubnum) for uubnum in uubset_all}
        uubset_exp = set([uubnum for uubnum in uubset_all
                          if isLive(uub2ip[uubnum], self.logger)])
        uubnums = []  # tested order of UUBs
        portmask = 1  # raw ports to switch off
        for n in range(9, -1, -1):  # expected max number of live UUBs
            self.pc.switchRaw(False, portmask)
            portmask <<= 1
            sleep(Evaluator.TOUT_ORD)
            uubset_real = set([uubnum for uubnum in uubset_all
                               if isLive(uub2ip[uubnum], self.logger)])
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
