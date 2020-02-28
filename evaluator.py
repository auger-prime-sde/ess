"""
  ESS procedure
  module for evaluation
"""

import threading
import logging
from datetime import datetime

from threadid import syscall, SYS_gettid
from UUB import VIRGINUUBNUM


class Evaluator(threading.Thread):
    """Evaluator for check internal SN & removeUUB"""
    ISN_SEVERITY_STRICT = 0   # require all UUBs correctly
    ISN_SEVERITY_I2CFAIL = 1  # allow I2C between zynq and SC failure
    ISN_SEVERITY_NOTLIVE = 2  # allow UUB not live
    ISN_SEVERITY_NODB = 4     # allow ISN not in DB
    ISN_SEVERITY_REPORT = 8   # no action, just report status

    def __init__(self, ctx, fp):
        """Constructor.
ctx - context object (i.e. ESS), used keys:
        timer
        uubnums
        internalSNs - dict
        uubtsc - uses uubtsc.internalSN
        critical_error - function to call to abort the test
fp - file/stream for output
"""
        super(Evaluator, self).__init__()
        self.timer = ctx.timer
        self.uubnums = ctx.uubnums
        self.dbISN = ctx.internalSNs
        self.uubtsc = ctx.uubtsc
        self.critical_error = ctx.critical_error
        self.removeUUB = ctx.removeUUB
        self.fp = fp
        self.thrs = []  # threads removing UUB to join
        self.logger = logging.getLogger('Evaluator')

    def run(self):
        tid = syscall(SYS_gettid)
        self.logger.debug('run start, name %s, tid %d', self.name, tid)
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

            if 'removeUUB' in flags:
                for uubnum in flags['removeUUB']:
                    thr = threading.Thread(
                        target=self.removeUUB,
                        args=(uubnum, self.logger))
                self.thrs.append(thr)
                thr.start()

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
        if zVirgin:
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

    def writeMsg(self, msglines, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now()
        ts = timestamp.strftime('%Y-%m-%dT%H:%M:%S | ')
        spacer = ' ' * len(ts)
        if msglines:
            self.fp.write(ts + msglines.pop(0) + '\n')
        for line in msglines:
            self.fp.write(spacer + line + '\n')

    def join(self, timeout=None):
        while self.thrs:
            try:
                thr = self.thrs.pop()
            except IndexError:
                break
            thr.join()
        super(Evaluator, self).join(timeout)
