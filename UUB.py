"""

 ESS procedure
 communication with UUB to get Zynq temperature and Slowcontrol data
 Implementation of UUB dispatcher & UUB meas
"""

import http.client
import logging
import re
import socket
import select
import threading
from datetime import datetime, timedelta
from time import sleep
from struct import unpack
from struct import error as struct_error
import telnetlib
import numpy as np

from dataproc import float2expo
from threadid import syscall, SYS_gettid

TELNETPORT = 23
HTTPPORT = 80
DATAPORT = 8888    # UDP port UUB send data to
CTRLPORT = 8887    # UDP port UUB listen for commands
ADCPORT = 8886     # UDP port adcramp on UUB communicates
LADDR = "192.168.31.254"  # IP address of the computer
VIRGINMAC = '00:0a:35:00:1e:53'
VIRGINIP = '192.168.31.0'
VIRGINUUBNUM = 0xF00
re_mac = re.compile(r'^00:0[aA]:35:00:([0-9]{2}):([0-9]{2})$')


def uubnum2mac(uubnum):
    """Calculate MAC address from UUB number"""
    if uubnum == 'xxxx' or uubnum == VIRGINUUBNUM:
        return VIRGINMAC
    assert 0 < uubnum < VIRGINUUBNUM, "Wrong UUB number"
    return '00:0a:35:00:%02d:%02d' % (uubnum // 100, uubnum % 100)


def mac2uubnum(mac):
    """ Calculate UUB number from MAC address"""
    if mac == VIRGINMAC:
        return VIRGINUUBNUM
    m = re_mac.match(mac)
    assert m is not None, 'Wrong MAC address'
    comps = [int(x, 10) for x in m.groups()]
    return 100*comps[0] + comps[1]


def uubnum2ip(uubnum):
    """Calculate IP address from UUB number"""
    if uubnum == 'xxxx' or uubnum == VIRGINUUBNUM:
        return VIRGINIP
    assert 0 < uubnum < VIRGINUUBNUM, "Wrong UUB number"
    return '192.168.%d.%d' % (16 + (uubnum >> 8), uubnum & 0xFF)


def ip2uubnum(ip):
    """Calculate UUB number from IP"""
    comps = [int(x) for x in ip.split('.')]
    assert comps[:2] == [192, 168]
    assert comps[2] & 0xF0 == 16
    uubnum = 0x100*(comps[2] & 0x0F) + comps[3]
    return uubnum


def gener_funcparams():
    """Return generators of AFG and item parameters
return [(timer.name, functype, generator, aflags), ...]
  aflags - parameters to store in q_resp
"""
    def generR(**kwargs):
        """Generator for ramp
kwargs: count
return afg_dict, item_dict"""
        item_dict = {'functype': 'R'}
        if 'count' in kwargs:
            for i in range(kwargs['count']):
                item_dict['index'] = i
                yield None, item_dict
        else:
            yield None, item_dict

    def generN(**kwargs):
        """Generator for noise
kwargs: count
return afg_dict, item_dict"""
        item_dict = {'functype': 'N'}
        if 'count' in kwargs:
            for i in range(kwargs['count']):
                item_dict['index'] = i
                yield None, item_dict
        else:
            yield None, item_dict

    def generP(**kwargs):
        """Generator for functype pulse
kwargs: splitmodes, voltages, count
return afg_dict, item_dict"""
        afg_dict = {'functype': 'P'}
        item_dict = afg_dict.copy()
        splitmodes = kwargs.get('splitmodes', (None, ))
        voltages = kwargs.get('voltages', (None, ))
        for splitmode in splitmodes:
            if splitmode is not None:
                item_dict['splitmode'] = splitmode
            for v in voltages:
                if v is not None:
                    afg_dict['Pvoltage'] = v
                    item_dict['voltage'] = v
                if 'count' in kwargs:
                    item_dict['index'] = 0
                    yield afg_dict, item_dict
                    for i in range(1, kwargs['count']):
                        item_dict['index'] = i
                        yield None, item_dict
                else:
                    yield afg_dict, item_dict

    def generF(**kwargs):
        """Generator for functype freq
kwargs: splitmodes, freqs, voltages, count
return afg_dict, item_dict"""
        afg_dict = {'functype': 'F'}
        item_dict = afg_dict.copy()
        splitmodes = kwargs.get('splitmodes', (None, ))
        freqs = kwargs.get('freqs', (None, ))
        voltages = kwargs.get('voltages', (None, ))
        for splitmode in splitmodes:
            if splitmode is not None:
                item_dict['splitmode'] = splitmode
            for freq in freqs:
                if freq is not None:
                    afg_dict['freq'] = freq
                    item_dict['freq'] = freq
                    item_dict['flabel'] = float2expo(freq)
                for v in voltages:
                    if v is not None:
                        afg_dict['Fvoltage'] = v
                        item_dict['voltage'] = v
                    if 'count' in kwargs:
                        item_dict['index'] = 0
                        yield afg_dict, item_dict
                        for i in range(1, kwargs['count']):
                            item_dict['index'] = i
                            yield None, item_dict
                    else:
                        yield afg_dict, item_dict

    return (('meas.ramp', 'R', generR),
            ('meas.pulse', 'P', generP),
            ('meas.freq', 'F', generF),
            ('meas.noise', 'N', generN))


def isLive(uub, timeout=0):
    """Try open TCP to UUB:80, eventually repeat until timeout expires.
Return True if UUB answers, False if timeout occurs.
uub - UUBmeas or UUBtsc (must have ip and logger attributes)
"""
    # uub.logger.debug('isLive(%f)', timeout)
    exptime = datetime.now() + timedelta(seconds=timeout)
    # uub.logger.debug('exptime = %s',
    #                  exptime.strftime("%Y-%m-%d %H:%M:%S,%f"))
    addr = (uub.ip, HTTPPORT)
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.settimeout(0.2)
            # uub.logger.debug('s.connect')
            s.connect(addr)
            # uub.logger.debug('s.close')
            # s.shutdown(socket.SHUT_RD)
            s.close()
            res = True
            break
        except (socket.timeout, socket.error):
            # uub.logger.debug('socket.timeout/error')
            # s.shutdown(socket.SHUT_RD)
            s.close()
            if datetime.now() > exptime:
                res = False
                break
    uub.logger.debug('isLive: %s', res)
    return res


class UUBtsc(threading.Thread):
    """Thread managing read out Zynq temperature and SlowControl data
 from UUB"""

    re_scdata = re.compile(r'''
   .* Power .* Nominal \s* Actual \s* Current \s*
   (10V   \s+ (?P<u_10V>\d+(\.\d+)?) \s* \[mV\] \s*)?
   1V    \s+ (?P<u_1V>\d+(\.\d+)?) \s* \[mV\] \s*
             (?P<i_1V>\d+(\.\d+)?) \s* \[mA\] \s*
   1V2   \s+ (?P<u_1V2>\d+(\.\d+)?) \s* \[mV\] \s*
             (?P<i_1V2>\d+(\.\d+)?) \s* \[mA\] \s*
   1V8   \s+ (?P<u_1V8>\d+(\.\d+)?) \s* \[mV\] \s*
             (?P<i_1V8>\d+(\.\d+)?) \s* \[mA\] \s*
   3V3   \s+ (?P<u_3V3>\d+(\.\d+)?) \s* \[mV\] \s*
             (?P<i_3V3>\d+(\.\d+)?) \s* \[mA\] \s*
             (?P<i_3V3_sc>\d+(\.\d+)?) \s* \[mA\ SC\] \s*
   P3V3  \s+ (?P<u_P3V3>\d+(\.\d+)?) \s* \[mV\] \s*
             (?P<i_P3V3>\d+(\.\d+)?) \s* \[mA\] \s*
   N3V3  \s+ -?(?P<u_N3V3>\d+(\.\d+)?) \s* \[mV\] \s*
             (?P<i_N3V3>\d+(\.\d+)?) \s* \[mA\] \s*
   5V    \s+ (?P<u_5V>\d+(\.\d+)?) \s* \[mV\] \s*
             (?P<i_5V>\d+(\.\d+)?) \s* \[mA\] \s*
   12V\ Radio \s+ (?P<u_radio>\d+(\.\d+)?) \s* \[mV\] \s*
                  (?P<i_radio>\d+(\.\d+)?) \s* \[mA\] \s*
   12V\ PMTs  \s+ (?P<u_PMTs>\d+(\.\d+)?) \s* \[mV\] \s*
                  (?P<i_PMTs>\d+(\.\d+)?) \s* \[mA\] \s*
   24V\ EXT1/2 \s+ (?P<u_ext1>\d+(\.\d+)?) \s* \[mV\] \s*
                   (?P<u_ext2>\d+(\.\d+)?) \s* \[mV\] \s*
                   (?P<i_ext>\d+(\.\d+)?) \s* \[mA\]
   .* Sensors \s+
   (T=\ (?P<temp>-?\d+(\.\d+)?)\ C \s*
    P=\ (?P<press>\d+(\.\d+)?)\ mBar \s*
    H=\ (?P<humid>\d+(\.\d+)?)\ \%)?
   (T= \s+ (?P<temp_dK>-?\d+) \s* \*0\.1K, \s*
    P= \s+ (?P<press1>\d+) \s* mBar)?
''', re.VERBOSE + re.DOTALL)

    def __init__(self, uubnum, timer, q_resp):
        """Constructor.
uubnum - UUB number
timer - instance of timer
q_resp - queue to send response
"""
        super(UUBtsc, self).__init__()
        self.uubnum = uubnum
        self.timer = timer
        self.q_resp = q_resp
        self.ip = uubnum2ip(uubnum)
        self.serial = None
        self.TIMEOUT = 5
        self.TRIALS = 3  # number of trials to read internal SN
        self.HTTP_TOUT = 3  # timeout for HTTP connections
        self.stopme = False
        self.logger = logging.getLogger('UUB-%04d' % uubnum)
        self.logger.info('UUBtsc created, IP %s.', self.ip)

    def run(self):
        tid = syscall(SYS_gettid)
        self.logger.debug('run start, name %s, tid %d', self.name, tid)
        self.logger.debug('Waiting for UUB being live')
        while self.serial is None:
            s = self.readSerialNum(self.TIMEOUT, self.TRIALS)
            if s is None:
                self.logger.debug('UUB not live yet, next try')
            elif s is False:
                self.logger.error('Cannot read internal SN')
                break
            else:
                self.serial = s
                dt = datetime.now().replace(microsecond=0)
                self.q_resp.put({'timestamp': dt,
                                 'internalSN_u%04d' % self.uubnum: s})
            if self.timer.stop.is_set() or self.stopme:
                self.logger.info('UUBtsc stopped')
                return
        # self.logger.info('added immediate telnet.login')
        # self.timer.add_immediate('telnet.login', [self.uubnum])
        self.logger.debug('UUB live, entering while loop')
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set() or self.stopme:
                self.logger.info('UUBtsc stopped')
                return
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            if ('meas.thp' not in flags and 'meas.sc' not in flags and
                    'power.test' not in flags):
                continue
            res = {'timestamp': timestamp}
            if 'power.test' in flags:
                res['live%04d' % self.uubnum] = isLive(self)
                if 'test_point' in flags:
                    res['test_point'] = flags['test_point']
            if 'meas.thp' in flags or 'meas.sc' in flags:
                self.logger.debug('Connecting UUB')
                conn = http.client.HTTPConnection(self.ip, HTTPPORT,
                                                  self.HTTP_TOUT)
                try:
                    # read Zynq temperature
                    if 'meas.thp' in flags:
                        res.update(self.readZynqTemp(conn))
                        res['meas_thp'] = True
                    # read SlowControl data
                    if 'meas.sc' in flags:
                        res.update(self.readSlowControl(conn))
                        res['meas_sc'] = True
                except (http.client.CannotSendRequest, socket.error,
                        AttributeError) as e:
                    self.logger.error('HTTP request failed, %s', e.__str__())
                finally:
                    conn.close()
                    self.logger.debug('HTTP connection closed')
            self.q_resp.put(res)

    def readSerialNum(self, timeout=None, trials=1):
        """Read UUB serial number
Return as 'ab-cd-ef-01-00-00' or None if UUB is not live"""
        re_sernum = re.compile(r'.*SN: (?P<sernum>' +
                               r'([a-fA-F0-9]{2}-){5}[a-fA-F0-9]{2})',
                               re.DOTALL)

        if timeout is not None and not isLive(self, timeout):
            return None
        self.logger.debug('Reading UUB serial number')
        while trials > 0:
            conn = http.client.HTTPConnection(self.ip, HTTPPORT,
                                              self.HTTP_TOUT)
            try:
                # self.logger.debug('sending conn.request')
                conn.request(
                    'GET', '/cgi-bin/getdata.cgi?action=slowc&arg1=-s')
                # self.logger.debug('conn.getresponse')
                resp = conn.getresponse().read().decode('ascii')
                # self.logger.debug('re_sernum')
                res = re_sernum.match(resp).groupdict()['sernum']
                # self.logger.debug('breaking')
            except AttributeError:
                res = False
            except (http.client.CannotSendRequest, socket.error):
                res = None
            finally:
                conn.close()
            if res is not None and res is not False:
                break
            trials -= 1
        return res

    def readZynqTemp(self, conn):
        """Read Zynq temperature: HTTP GET + parse
conn - HTTPConnection instance
return dictionary: zynq<uubnum>_temp: temperature
"""
        re_zynqtemp = re.compile(
            r'{"Zynq": (?P<zt>[+-]?\d+(\.\d*)?)}')
        conn.request('GET', '/cgi-bin/getdata.cgi?action=xadc')
        # TO DO: check status
        resp = conn.getresponse().read().decode('ascii')
        self.logger.debug('xadc GET: "%s"', repr(resp))
        m = re_zynqtemp.match(resp)
        if m is not None:
            return {'zynq%04d_temp' % self.uubnum: float(m.groupdict()['zt'])}
        self.logger.warning('Resp to xadc does not match Zynq temperature')
        return {}

    def readSlowControl(self, conn):
        """Read Slow Control data: HTTP GET + parse
conn - HTTPConnection instance
return dictionary: sc<uubnum>_<variable>: value
"""
        conn.request('GET', '/cgi-bin/getdata.cgi?action=slowc&arg1=-a')
        self.logger.debug('req sent')   # DEBUG
        # TO DO: check status
        resp = conn.getresponse().read().decode('ascii')
        self.logger.debug('slowc GET: "%s"', repr(resp))
        m = self.re_scdata.match(resp)
        if m is not None:
            # prefix keys
            prefix = 'sc%04d_' % self.uubnum
            res = {prefix+k: float(v) for k, v in m.groupdict().items()
                   if v is not None}
            # transform 0.1K -> deg.C for UUB v1
            if prefix+'temp_dK' in res:
                res[prefix+'temp'] = 0.1 * res.pop(prefix+'temp_dK') - 273.15
                res[prefix+'press'] = res.pop(prefix+'press1')
        else:
            self.logger.warning('Resp to slowc -a does not match expected')
            res = {}
        return res


class UUBdaq(threading.Thread):
    """Thread managing data acquisition from UUBs"""
    TOUT_PREP = 0.2   # delay between afg setting and trigger in s
    TOUT_RAMP = 0.05  # delay between setting ADC ramp and trigger in s
    TOUT_DAQ = 0.1    # timeout between trigger and UUBlisten cancel

    def __init__(self, timer, ulisten, q_resp, q_ndata,
                 afg, splitmode, spliton, trigdelay, trigger,
                 gener_param=gener_funcparams()):
        """Constructor
timer - instance of timer
ulisten - instance of UUBlistener
q_resp - queue for responses (for meas_point/meas_<name>/db_<name>
q_ndata - queue to DataProcessors
afg - instance of AFG
splitmode - bound method PowerControl.splitterMode
spliton - bound method to power on/off splitter
trigdelay - instance of TrigDelay
trigger - bound method for trigger
gener_param - generator of measurement paramters (see gener_funcparams)
"""
        super(UUBdaq, self).__init__()
        self.timer, self.ulisten = timer, ulisten
        self.q_resp, self.q_ndata = q_resp, q_ndata
        self.afg, self.splitmode, self.spliton = afg, splitmode, spliton
        self.trigger, self.trigdelay = trigger, trigdelay
        self.tnames = [rec[0] for rec in gener_param]  # timer names
        self.functypes = {rec[0]: rec[1] for rec in gener_param}
        self.geners = {rec[0]: rec[2] for rec in gener_param}
        self.uubnums = set()
        self.uubnums2add = []
        self.uubnums2del = []
        self.adcramp = {}

    def run(self):
        logger = logging.getLogger('UUBdaq')
        tid = syscall(SYS_gettid)
        logger.debug('run start, name %s, tid %d', self.name, tid)
        # stop and clear ulisten
        self.ulisten.uubnums = set()
        self.ulisten.clear = True
        self.ulisten.permanent = False
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                break
            # store timer parameters
            timestamp = self.timer.timestamp   # store info from timer
            tflags = {tname: self.timer.flags[tname]
                      for tname in self.tnames
                      if tname in self.timer.flags}
            if not tflags:
                continue

            # update uubnums
            while self.uubnums2add:
                uubnum = self.uubnums2add.pop()
                self.uubnums.add(uubnum)
                self.adcramp[uubnum] = ADCramp(uubnum)
            while self.uubnums2del:
                uubnum = self.uubnums2del.pop()
                self.uubnums.discard(uubnum)
                del self.adcramp[uubnum]

            # loop over functype/tname
            for tname in self.tnames:
                if tname not in tflags:
                    continue
                logger.info('executing %s, flags %s',
                            tname, repr(tflags[tname]))
                if self.trigdelay is not None:
                    self.trigdelay.delay = self.functypes[tname]
                if tname == 'meas.ramp':
                    for adcr in self.adcramp.values():
                        adcr.switchOn()
                    sleep(UUBdaq.TOUT_RAMP)
                elif tname == 'meas.noise':
                    if self.spliton is not None:
                        logger.info('splitter power off for meas.noise')
                        self.spliton(False)
                        sleep(UUBdaq.TOUT_PREP)
                elif tname in ('meas.pulse', 'meas.freq'):
                    self.afg.switchOn(True)
                # run measurement for all parameters
                for afg_dict, item_dict in self.geners[tname](**tflags[tname]):
                    logger.debug("params %s", repr(item_dict))
                    item_dict['timestamp'] = timestamp
                    self.ulisten.done.clear()
                    self.ulisten.details = item_dict.copy()
                    self.ulisten.uubnums = self.uubnums.copy()
                    if afg_dict is not None:
                        self.afg.setParams(**afg_dict)
                        if 'splitmode' in item_dict:
                            self.splitmode(item_dict['splitmode'])
                        sleep(UUBdaq.TOUT_PREP)
                    self.trigger()
                    logger.debug('trigger sent')
                    finished = self.ulisten.done.wait(UUBdaq.TOUT_DAQ)
                    if not finished:
                        logger.debug('timeout')
                        # debug UUBlisten.records before cleaning
                        self.ulisten.logrecords = True
                    # stop daq at ulisten
                    self.ulisten.uubnums = set()
                    self.ulisten.clear = True
                    self.ulisten.cleared.wait()
                    logger.debug('DAQ completed')
                if tname == 'meas.ramp':
                    for adcr in self.adcramp.values():
                        adcr.switchOff()
                    sleep(UUBdaq.TOUT_RAMP)
                    # wait until all ramp traces are processed
                    self.q_ndata.join()
                elif tname == 'meas.noise':
                    if self.spliton is not None:
                        logger.info('splitter power on after meas.noise')
                        self.spliton(True)
                        sleep(UUBdaq.TOUT_PREP)
                elif tname in ('meas.pulse', 'meas.freq'):
                    self.afg.switchOn(False)
        # end while(True)
        logger.info('Timer stopped, stopping UUB daq')


class UUBlisten(threading.Thread):
    """Listen for UDP packets with data from UUB"""
    def __init__(self, q_ndata):
        """Constructor.
q_ndata - a queue to send received data (NetscopeData instance)"""
        super(UUBlisten, self).__init__()
        self.q_ndata = q_ndata
        self.stop = threading.Event()
        self.done = threading.Event()
        self.cleared = threading.Event()
        # adjust before run
        self.port = DATAPORT
        self.laddr = LADDR
        self.PACKETSIZE = 1500
        self.RCVBUF = 1000000  # size of UDP socket recv buffer in bytes
        self.SLEEPTIME = 0.01  # timeout for checking active event
        self.NPOINT = 2048    # number of measured points
        self.details = None
        self.uubnums = set()  # UUBs to monitor
        # if False, remove UUBnum from uubnums after a header received
        self.permanent = True
        self.clear = False    # when True, discard all records
        self.logrecords = False    # when True, log records before discarding
        self.records = {}

    def run(self):
        logger = logging.getLogger('UUBlisten')
        tid = syscall(SYS_gettid)
        logger.debug('run start, name %s, tid %d', self.name, tid)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.laddr, self.port))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.RCVBUF)
        self.sock.settimeout(self.SLEEPTIME)
        logger.info("Listening on %s:%d", self.laddr, self.port)
        while not self.stop.is_set():
            try:
                data, addr = self.sock.recvfrom(self.PACKETSIZE)
            except socket.timeout:
                # logger.debug('socket timeout')
                continue
            finally:
                if self.clear:
                    if self.logrecords:
                        reclog = ', '.join([
                            '(UUB %d, port %d, id %08x): ' % key +
                            rec.__str__()
                            for key, rec in self.records.items()])
                        logger.debug('Discarding records: { %s }', reclog)
                        self.logrecords = False
                    self.records = {}
                    self.clear = False
                    self.cleared.set()
            nsid = unpack('<L', data[:4])[0]
            uubnum = ip2uubnum(addr[0])
            # (UUBnum, port, id)
            key = (uubnum, addr[1], nsid & 0x7FFFFFFF)
            # logger.debug('packet UUB %d, port %d, id %08x',
            #              key[0], key[1], nsid)
            if nsid & 0x80000000:  # header
                if key in self.records:
                    logger.error('duplicate header (UUB %d, port %d, id %08x)',
                                 *key)
                    continue
                elif uubnum not in self.uubnums:
                    logger.debug(
                        'unsolicited header (UUB %d, port %d, id %08x)', *key)
                    continue
                else:
                    try:
                        self.records[key] = NetscopeData(data, uubnum,
                                                         self.details)
                        if not self.permanent:
                            self.uubnums.discard(uubnum)
                        logger.info(
                            'new record UUB %d, port %d, id %08x, rd%d',
                            key[0], key[1], key[2], self.records[key].rd)
                    except struct_error:
                        logger.error('header length error (%d) ' +
                                     'from UUB %d, port %d, id %08x',
                                     len(data), *key)
            else:   # chunk
                if key in self.records:
                    try:
                        if self.records[key].addChunk(data):
                            # send to q_ndata
                            nd = self.records.pop(key)
                            nd.cover = None
                            self.q_ndata.put(nd)
                            logger.info('done record UUB %d, port %d, id %08x',
                                        *key)
                            if not self.uubnums and not self.records:
                                self.done.set()
                    except ValueError as e:
                        logger.error('addChunk error %s, ' +
                                     'UUB %d, port %d, id %08x',
                                     e.__str__(), *key)
                else:
                    cid, start, end = NetscopeData.chunkHead(data)
                    logger.debug('orphan chunk for UUB %d, port %d,' +
                                 ' id %08x [%04x:%04x]',
                                 key[0], key[1], cid, start, end)
        logger.info("Leaving run()")
        self.sock.close()


class Coverage(object):
    """Cover range(0, MAX) by chunks."""
    def __init__(self, size):
        self.size = size
        self.starts = []
        self.ends = []

    def insert(self, start, end):
        """Add a chunk in coverage.
Return False if overlapping or outside, True otherwise."""
        if not 0 <= start < end <= self.size:
            return False
        curLen = len(self.starts)
        pos = len([i for i in self.ends if start >= i])
        if pos < curLen and end > self.starts[pos]:
            return False
        # ok, insert the new chunk
        self.starts.insert(pos, start)
        self.ends.insert(pos, end)
        # check if a merge is possible
        if pos < curLen and end == self.starts[pos+1]:
            self.starts.pop(pos+1)
            self.ends.pop(pos)
        if pos > 0 and start == self.ends[pos-1]:
            self.starts.pop(pos)
            self.ends.pop(pos-1)
        return True

    def isCovered(self):
        """Return True if (0, MAX) completely covered."""
        return len(self.starts) == 1 and \
            self.starts[0] == 0 and self.ends[0] == self.size

    def __str__(self):
        return "coverage(0, %d): " % self.size + \
            ", ".join(["(%d, %d)" % (start, end)
                       for start, end in zip(self.starts, self.ends)])


class NetscopeData(object):
    """ Data received from netscope """
    HEADER = ('id', 'shwr_buf_status', 'shwr_buf_start', 'shwr_buf_trig_id',
              'ttag_shwr_seconds', 'ttag_shwr_nanosec', 'rd')
    NPOINT = 2048
    RAWDATASIZE = 4 * 5 * NPOINT
    FRAGHEADLEN = 8     # LHH: id, start, end

    def __init__(self, header, uubnum, details=None):
        """Constructor.
header - data as in `struct shwr_header'"""
        headerdata = unpack('<%dL' % len(NetscopeData.HEADER), header)
        headerdict = dict(zip(NetscopeData.HEADER, headerdata))
        self.__dict__.update(headerdict)
        self.id &= 0x7FFFFFFF
        self.uubnum = uubnum
        self.details = details if details is not None else {
            'timestampmicro': datetime.now()}
        self.rawdata = bytearray(NetscopeData.RAWDATASIZE)
        self.yall = None
        self.cover = Coverage(NetscopeData.RAWDATASIZE)

    @staticmethod
    def chunkHead(chunk):
        """Return cid, start, end of the chunk"""
        return unpack('<LHH', chunk[:NetscopeData.FRAGHEADLEN])

    def addChunk(self, chunk):
        """Add a chunk into data. Return True if data complete."""
        # fragment header
        cid, start, end = NetscopeData.chunkHead(chunk)
        if len(chunk) - NetscopeData.FRAGHEADLEN != end - start:
            raise ValueError("Wrong start/end versus chunk length")
        if cid != self.id:
            raise ValueError("Wrong id %d (%d expected)" % (cid, self.id))
        if not self.cover.insert(start, end):
            raise ValueError("Incompatible chunk (%d, %d), already covered %s"
                             % (start, end, self.cover.__str__()))
        self.rawdata[start:end] = bytearray(chunk[NetscopeData.FRAGHEADLEN:])
        return self.cover.isCovered()

    def header(self):
        """Return header as dictionary"""
        d = {key: self.__dict__[key] for key in self.HEADER}
        # d['uubnum'] = self.uubnum
        # if self.details is not None:
        #     d.update(self.details)
        return d

    def convertData(self):
        """Convert raw data to numpy 2048x10 array"""
        if self.yall is not None:
            return self.yall
        yall = np.zeros([self.NPOINT, 10], dtype=float)
        start = self.shwr_buf_start
        for i in range(self.NPOINT):
            index = (i + start) % self.NPOINT
            for j in range(5):
                off = (j*self.NPOINT+index)*4
                hg, lg = unpack("<HH", self.rawdata[off:off+4])
                yall[i, 2*j] = hg & 0xFFF
                yall[i, 2*j+1] = lg & 0xFFF
        self.yall = yall
        return yall

    def __str__(self):
        return ("NetscopeData(uubnum=%04d, cid=0x%08x, details=%s, " +
                "coverage=%s)") % (self.uubnum, self.id, repr(self.details),
                                   self.cover.__str__())


class UUBtelnet(threading.Thread):
    """Class making telnet to UUBs and run netscope program"""

    def __init__(self, timer, uubnums, dloadfn=None):
        super(UUBtelnet, self).__init__()
        self.timer = timer
        self.uubnums = list(uubnums)
        self.telnets = [None] * len(self.uubnums)
        self.uubnums2add = []
        self.uubnums2del = []
        self.logger = logging.getLogger('UUBtelnet')
        self.dloadfp = None
        if dloadfn is not None:
            self.logger.info('Opening %s for downloads', dloadfn)
            self.dloadfp = open(dloadfn, "ab")
        self.timestamp = None
        # parameters
        self.TOUT = 1  # timeout for read_until
        self.TOUT_CMD = 0.1  # timeout between cmds and downloads
        self.LOGIN = "root"
        self.PASSWD = "root"
        self.PROMPT = "#"     # prompt to expect after successfull login

    def _read_until(self, tn, match):
        """Telnet.read_until but raise AssertionError if does not match
match - bytes or bytearray"""
        resp = tn.read_until(match, self.TOUT)
        assert resp.find(match) >= 0
        return resp

    def _isdead(self, tn):
        """Check if underlying socket is dead by sending IAC+NOP
tn - instance of Telnet"""
        try:
            # the first sendall() causes socket closing
            tn.sock.sendall(telnetlib.IAC + telnetlib.NOP)
            # the second sendall() raises exception because of closed socket
            tn.sock.sendall(telnetlib.IAC + telnetlib.NOP)
            return False
        except ConnectionResetError:
            return True

    def _login(self, uubnums=None):
        """Login to UUBs
uubnums - if not None, logs in only to these UUB
return list of failed UUBs or None"""
        failed = []
        for ind, uubnum in enumerate(self.uubnums):
            if uubnums is not None and uubnum not in uubnums:
                continue
            tn = self.telnets[ind]
            if tn is not None:   # close previously open telnet
                self.logger.debug('closing UUB %04d before login', uubnum)
                tn.close()
            try:
                self.logger.debug('logging to UUB %04d', uubnum)
                tn = telnetlib.Telnet(uubnum2ip(uubnum), TELNETPORT, self.TOUT)
                self._read_until(tn, b"login: ")
                tn.write(bytes(self.LOGIN, 'ascii') + b"\n")
                self._read_until(tn, b"Password: ")
                tn.write(bytes(self.PASSWD, 'ascii') + b"\n")
                self._read_until(tn, bytes(self.PROMPT, 'ascii'))
                self.telnets[ind] = tn
            except (socket.error, EOFError, AssertionError):
                self.logger.warning('logging to UUB %04d failed', uubnum)
                self.telnets[ind] = None
                failed.append(uubnum)
        return failed if failed else None

    def _logout(self, uubnums=None):
        """Logout from UUBs
uubnums - if not None, logs out only from these UUB"""
        for ind, uubnum in enumerate(self.uubnums):
            if uubnums is not None and uubnum not in uubnums:
                continue
            tn = self.telnets[ind]
            if tn is None:   # close previously open telnet
                self.logger.debug('UUB %04d already closed', uubnum)
            else:
                self.logger.debug('logging off UUB %04d', uubnum)
                tn.close()
                self.telnets[ind] = None

    def _runcmds(self, cmdlist, uubnums=None):
        """Run commands on UUBs
cmdlist - list of commands to run
uubnums - if not None, logs in only to these UUB
return list of failed UUBs or None"""
        failed = []
        for ind, uubnum in enumerate(self.uubnums):
            if uubnums is not None and uubnum not in uubnums:
                continue
            tn = self.telnets[ind]
            if tn is None or self._isdead(tn):
                if tn is None:
                    self.logger.warning(
                        'not logged to UUB %04d yet, logging in', uubnum)
                else:
                    self.logger.warning(
                        'telnet to UUB %04d dead, logging out/in', uubnum)
                    tn.close()
                if self._login(uubnums=(uubnum, )) is not None:
                    failed.append(uubnum)
                    continue
                tn = self.telnets[ind]
            for cmd in cmdlist:
                try:
                    self.logger.debug('command to UUB %04d: "%s"',
                                      uubnum, cmd)
                    bcmd = bytes(cmd, 'ascii')
                    tn.write(bcmd + b"\n")
                    self._read_until(tn, bcmd + b"\r\n")
                except (socket.error, EOFError, AssertionError):
                    self.logger.warning('sending commands to UUB %04d failed',
                                        uubnum)
                    self.telnets[ind] = None
                    failed.append(uubnum)
                    break  # for cmd in cmdlist
        return failed if failed else None

    def _downloads(self, filelist, uubnums=None):
        """Download requested files from UUBs via HTTP
filelist - list of files to download
uubnums - if not None, logs in only to these UUB
return list of tuples with failed UUB/file or None"""
        if self.dloadfp is None:
            self.logger.error('Download file not open')
            return None
        failed = []
        if uubnums is None:
            uubnums = self.uubnums
        if self.timestamp is not None:
            ts = self.timestamp.strftime("ts=%Y-%m-%dT%H:%M:%S ")
        else:
            ts = ""
        for uubnum in uubnums:
            conn = http.client.HTTPConnection(uubnum2ip(uubnum), HTTPPORT)
            for f in filelist:
                self.logger.debug('Downloading %s from UUB #%04d', f, uubnum)
                try:
                    conn.request('GET', '/' + f)
                    resp = conn.getresponse()
                    if resp.status != 200:
                        data = b''
                        size = -1
                    else:
                        data = resp.read()
                        size = len(data)
                    header = "=*= %suubnum=%04d filename=%s size=%d =*=\n" % (
                            ts, uubnum, f, size)
                    self.dloadfp.write(bytes(header, 'ascii'))
                    self.dloadfp.write(data)
                except (http.client.CannotSendRequest, socket.error,
                        AttributeError) as e:
                    self.logger.error('Download failed, %s', e.__str__())
                    failed.append((uubnum, f))
            self.logger.debug('Closing HTTP connection to UUB #%04d', uubnum)
            conn.close()
        self.dloadfp.flush()
        return failed if failed else None

    def __del__(self):
        for tn in self.telnets:
            if tn is not None:
                tn.close()
        if self.dloadfp is not None:
            self.dloadfp.close()
            self.dloadfp = None

    def run(self):
        tid = syscall(SYS_gettid)
        self.logger.debug('run start, name %s, tid %d', self.name, tid)
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                self.logger.info('Timer stopped, closing telnets')
                self._logout()
                return
            self.timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            while self.uubnums2add:
                uubnum = self.uubnums2add.pop()
                if uubnum in self.uubnums:
                    continue
                self.uubnums.append(uubnum)
                self.telnets.append(None)
            while self.uubnums2del:
                uubnum = self.uubnums2del.pop()
                if uubnum not in self.uubnums:
                    continue
                ind = self.uubnums.index(uubnum)
                tn = self.telnets.pop(ind)
                if tn is not None:
                    tn.close()
                self.uubnums.pop(ind)
            if 'telnet.logout' in flags:
                self.logger.info('logout event')
                self._logout(flags['telnet.logout'])
            if 'telnet.login' in flags:
                self.logger.info('login event')
                self._login(flags['telnet.login'])
            if 'telnet.cmds' in flags:
                self.logger.info('telnet commands')
                cmdlist = flags['telnet.cmds']['cmdlist']
                uubnums = flags['telnet.cmds'].get('uubnums', None)
                self._runcmds(cmdlist, uubnums=uubnums)
                sleep(self.TOUT_CMD)  # ad hoc before downloads
            if 'telnet.dloads' in flags:
                self.logger.info('telnet downloads')
                filelist = flags['telnet.dloads']['filelist']
                uubnums = flags['telnet.dloads'].get('uubnums', None)
                self._downloads(filelist, uubnums=uubnums)


def ADCtup2c(tup):
    """Convert (adc, on/off, channelsel) tuple to char"""
    chsel = 0
    if 'A' in tup[2]:
        chsel += 1
    if 'B' in tup[2]:
        chsel += 2
    on = 0x20 if tup[1] else 0
    adc = int(tup[0])
    assert 0 <= adc < 5
    return chr(0x40 + on + (adc << 2) + chsel)


class ADCramp(object):
    """Switch ADC to/from ramp test mode"""
    MSGLEN = 18   # length of UDP payload (minimal without padding)

    def __init__(self, uubnum):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.01)
        self.addr = (uubnum2ip(uubnum), ADCPORT)
        self.logger = logging.getLogger('ADCramp %04d' % uubnum)

    allON = ''.join([ADCtup2c((adc, True, 'AB')) for adc in range(5)])
    allOFF = ''.join([ADCtup2c((adc, False, 'AB')) for adc in range(5)])

    def _send_recv(self, cmd):
        """Send command, receive response and check it.
cmd - str to send
If OK, return True, else return False"""
        clen = len(cmd)
        assert clen < ADCramp.MSGLEN
        self.logger.debug('emptying recv buf')
        self._empty_socket()
        self.logger.debug('sending %s', repr(cmd))
        msg = bytes(cmd, 'ascii') + bytes(ADCramp.MSGLEN - clen)
        self.sock.sendto(msg, self.addr)
        try:
            resp, addr = self.sock.recvfrom(ADCramp.MSGLEN)
        except socket.timeout:
            self.logger.info('timeout')
            return False
        expresp = 0x20 + clen
        if resp[0] != expresp:
            self.logger.info('Unexpected response %02X (%02X expected)',
                             resp[0], expresp)
            return False
        self.logger.debug('done OK')
        return True

    def _empty_socket(self):
        """remove the data present on the socket"""
        input = [self.sock]
        while 1:
            inputready, o, e = select.select(input, [], [], 0.0)
            if len(inputready) == 0:
                break
            for s in inputready:
                s.recv(1)

    def switchOn(self):
        """Switch all ADCs to ramp mode"""
        self._send_recv(ADCramp.allON)

    def switchOff(self):
        """Switch all ADCs back to normal mode"""
        self._send_recv(ADCramp.allOFF)

    def kill(self):
        """Kill adcramp deamon on UUB"""
        self._send_recv('!')
