"""

 ESS procedure
 communication with UUB to get Zynq temperature and Slowcontrol data
 Implementation of UUB dispatcher & UUB meas
"""

import httplib
import logging
import re
import socket
import threading
from datetime import datetime, timedelta
from time import sleep
from struct import unpack
from struct import error as struct_error
from Queue import Empty
from telnetlib import Telnet
import numpy

HTTPPORT = 80
DATAPORT = 8888    # UDP port UUB send data to
CTRLPORT = 8887    # UDP port UUB listen for commands
LADDR = "192.168.31.254"  # IP address of the computer


def uubnum2ip(uubnum):
    """Calculate IP address from UUB number"""
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
    def generP(**kwargs):
        """Generator for functype pulse
kwargs: ch2s, voltages
return afg_dict, item_dict"""
        afg_dict = {'functype': 'P'}
        item_dict = afg_dict.copy()
        ch2s = kwargs.get('ch2s', (None, ))
        voltages = kwargs.get('voltages', (None, ))
        for ch2 in ch2s:
            if ch2 is not None:
                afg_dict['ch2'] = ch2
                item_dict['ch2'] = ch2
            for v in voltages:
                if v is not None:
                    afg_dict['Pvoltage'] = v
                    item_dict['voltage'] = v
                yield afg_dict, item_dict

    def generF(**kwargs):
        """Generator for functype freq
kwargs: ch2s, freqs, voltages
return afg_dict, item_dict"""
        afg_dict = {'functype': 'F'}
        item_dict = afg_dict.copy()
        ch2s = kwargs.get('ch2s', (None, ))
        freqs = kwargs.get('freqs', (None, ))
        voltages = kwargs.get('voltages', (None, ))
        for ch2 in ch2s:
            if ch2 is not None:
                afg_dict['ch2'] = ch2
                item_dict['ch2'] = ch2
            for freq in freqs:
                if freq is not None:
                    afg_dict['freq'] = freq
                    item_dict['freq'] = freq
                for v in voltages:
                    if v is not None:
                        afg_dict['Fvoltage'] = v
                        item_dict['voltage'] = v
                    yield afg_dict, item_dict

    return (('meas.pulse', 'P', generP,
             ('meas_pulse_point', 'db_pulse')),
            ('meas.freq', 'F', generF,
             ('meas_freq_point', 'db_freq')))


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
            s.settimeout(1.0)
            # uub.logger.debug('s.connect')
            s.connect(addr)
            # uub.logger.debug('s.close')
            s.close()
            # uub.logger.debug('isLive: True')
            return True
        except (socket.timeout, socket.error):
            # uub.logger.debug('socket.timeout/error')
            s.close()
            if datetime.now() > exptime:
                # uub.logger.debug('isLive: False')
                return False


class UUBtsc(threading.Thread):
    """Thread managing read out Zynq temperature and SlowControl data
 from UUB"""

    re_scdata = re.compile(r'''.*
   PMT1 \s+ (?P<HV_PMT1>\d+(\.\d+)?)
        \s+ (?P<I_PMT1>\d+(\.\d+)?)
        \s+ (?P<T_PMT1>\d+(\.\d+)?) \s*
   PMT2 \s+ (?P<HV_PMT2>\d+(\.\d+)?)
        \s+ (?P<I_PMT2>\d+(\.\d+)?)
        \s+ (?P<T_PMT2>\d+(\.\d+)?) \s*
   PMT3 \s+ (?P<HV_PMT3>\d+(\.\d+)?)
        \s+ (?P<I_PMT3>\d+(\.\d+)?)
        \s+ (?P<T_PMT3>\d+(\.\d+)?) \s*
   PMT4 \s+ (?P<HV_PMT4>\d+(\.\d+)?)
        \s+ (?P<I_PMT4>\d+(\.\d+)?)
        \s+ (?P<T_PMT4>\d+(\.\d+)?)  \s*
   .* Power .* Nominal .*
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
   T= \s+ (?P<temp>\d+) \s* \*0\.1K, \s*
   P= \s+ (?P<press>\d+) \s* mBar
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
        self.logger = logging.getLogger('UUB-%04d' % uubnum)
        self.logger.info('UUBtsc created, IP %s.', self.ip)

    def run(self):
        self.logger.debug('Waiting for UUB being live')
        while self.serial is None:
            s = self.readSerialNum(self.TIMEOUT)
            if s is None:
                self.logger.debug('UUB not live yet, next try')
            else:
                self.serial = s
            if self.timer.stop.is_set():
                self.logger.info('UUBtsc stopped')
                return
        self.logger.debug('UUB live, entering while loop')
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
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
            self.logger.debug('Connecting UUB')
            conn = httplib.HTTPConnection(self.ip, HTTPPORT)
            try:
                # read Zynq temperature
                if 'meas.thp' in flags:
                    res.update(self.readZynqTemp(conn))
                # read SlowControl data
                if 'meas.sc' in flags:
                    res.update(self.readSlowControl(conn))
            except (httplib.CannotSendRequest, socket.error,
                    AttributeError) as e:
                self.logger.error('HTTP request failed, %s', e.__str__())
            finally:
                conn.close()
                self.logger.debug('HTTP connection closed')
            self.q_resp.put(res)

    def readSerialNum(self, timeout=None):
        """Read UUB serial number
Return as 'ab-cd-ef-01-00-00' or None if UUB is not live"""
        re_sernum = re.compile(r'.*\nSN: (?P<sernum>' +
                               r'([a-fA-F0-9]{2}-){5}[a-fA-F0-9]{2})',
                               re.DOTALL)

        if timeout is not None and not isLive(self, timeout):
            return None
        self.logger.debug('Reading UUB serial number')
        conn = httplib.HTTPConnection(self.ip, HTTPPORT)
        try:
            # self.logger.debug('sending conn.request')
            conn.request('GET', '/cgi-bin/getdata.cgi?action=slowc&arg1=-s')
            # self.logger.debug('conn.getresponse')
            resp = conn.getresponse().read()
            # self.logger.debug('re_sernum')
            res = re_sernum.match(resp).groupdict()
            # self.logger.debug('breaking')
        except (httplib.CannotSendRequest, socket.error, AttributeError):
            conn.close()
            return None
        conn.close()
        return res

    def readZynqTemp(self, conn):
        """Read Zynq temperature: HTTP GET + parse
conn - HTTPConnection instance
return dictionary: zynq<uubnum>_temp: temperature
"""
        re_zynqtemp = re.compile(
            r'Zynq temperature: (?P<zt>[+-]?\d+(\.\d*)?) degrees')
        conn.request('GET', '/cgi-bin/getdata.cgi?action=xadc')
        # TO DO: check status
        resp = conn.getresponse().read()
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
        # TO DO: check status
        resp = conn.getresponse().read()
        self.logger.debug('slowc GET: "%s"', repr(resp))
        m = self.re_scdata.match(resp)
        if m is not None:
            # prefix keys
            prefix = 'sc%04d_' % self.uubnum
            res = {prefix+k: float(v) for k, v in m.groupdict().iteritems()}
            # transform 0.1K -> deg.C
            res[prefix+'temp'] = 0.1 * res[prefix+'temp'] - 273.15
        else:
            self.logger.warning('Resp to slowc -a does not match expected')
            res = {}
        return res


class UUBdaq(threading.Thread):
    """Thread managing data acquisition from UUBs"""
    TOUT_PREP = 0.2   # delay between afg setting and trigger in s
    TOUT_DAQ = 0.1    # timeout between trigger and UUBlisten cancel

    def __init__(self, timer, afg, ulisten, trigdelay, q_resp,
                 gener_param=gener_funcparams()):
        """Constructor
timer - instance of timer
afg - instance of AFG
ulisten - instance of UUBlistener
trigdelay - instance of TrigDelay
gener_param - generator of measurement paramters (see gener_funcparams)
"""
        super(UUBdaq, self).__init__()
        self.timer, self.afg, self.ulisten = timer, afg, ulisten
        self.trigdelay, self.q_resp = trigdelay, q_resp
        self.tnames = [rec[0] for rec in gener_param]  # timer names
        self.functypes = {rec[0]: rec[1] for rec in gener_param}
        self.geners = {rec[0]: rec[2] for rec in gener_param}
        self.aflags = {rec[0]: rec[3] for rec in gener_param}
        self.ulisten = ulisten
        self.uubnums = set()
        self.uubnums2add = []
        self.uubnums2del = []

    def run(self):
        logger = logging.getLogger('UUBdaq')
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
            # copy other relevant flags
            aflags = {'timestamp': timestamp}
            for tname, flags in tflags.iteritems():
                aflags.update({key: flags[key] for key in self.aflags[tname]
                               if key in flags})
            if len(aflags) > 1:
                self.q_resp.put(aflags)

            # update uubnums
            while self.uubnums2add:
                self.uubnums.add(self.uubnums2add.pop())
            while self.uubnums2del:
                self.uubnums.discard(self.uubnums2del.pop())

            # loop over functype/tname
            for tname in self.tnames:
                if tname not in tflags:
                    continue
                logger.info('executing %s, flags %s',
                            tname, repr(tflags[tname]))
                self.trigdelay.delay = self.functypes[tname]
                # run measurement for all parameters
                for afg_dict, item_dict in self.geners[tname](**tflags[tname]):
                    logger.debug("params %s", repr(item_dict))
                    item_dict['timestamp'] = timestamp
                    self.afg.setParams(**afg_dict)
                    self.afg.switchOn(True)
                    self.ulisten.done.clear()
                    self.ulisten.details = item_dict.copy()
                    self.ulisten.uubnums = self.uubnums.copy()
                    sleep(UUBdaq.TOUT_PREP)
                    self.afg.trigger()
                    logger.debug('trigger sent')
                    finished = self.ulisten.done.wait(UUBdaq.TOUT_DAQ)
                    if not finished:
                        logger.debug('timeout')
                    # stop daq at ulisten
                    self.ulisten.uubnums = set()
                    self.ulisten.clear = True
                    self.afg.switchOn(False)
                    logger.debug('DAQ completed')
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
        # adjust before run
        self.port = DATAPORT
        self.laddr = LADDR
        self.PACKETSIZE = 1500
        self.SLEEPTIME = 0.001  # timeout for checking active event
        self.NPOINT = 2048    # number of measured points
        self.details = None
        self.uubnums = set()  # UUBs to monitor
        # if False, remove UUBnum from uubnums after a header received
        self.permanent = True
        self.clear = False    # when True, discard all records
        self.records = {}

    def run(self):
        logger = logging.getLogger('UUBlisten')
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.laddr, self.port))
        self.sock.settimeout(0.0001)
        logger.info("Listening on %s:%d", self.laddr, self.port)
        while not self.stop.is_set():
            if self.clear:
                self.records = {}
                self.clear = False
            try:
                data, addr = self.sock.recvfrom(self.PACKETSIZE)
            except socket.timeout:
                continue
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
                        logger.info('new record UUB %d, port %d, id %08x',
                                    *key)
                    except struct_error:
                        logger.error('header length error (%d) ' +
                                     'from UUB %d, port %d, id %08x',
                                     len(data), *key)
            else:   # chunk
                if key in self.records:
                    try:
                        if self.records[key].addChunk(data):
                            # send to q_ndata
                            self.q_ndata.put(self.records.pop(key))
                            logger.info('done record UUB %d, port %d, id %08x',
                                        *key)
                            if not self.uubnums and not self.records:
                                self.done.set()
                    except ValueError as e:
                        logger.error('addChunk error %s, ' +
                                     'UUB %d, port %d, id %08x',
                                     e.__str__(), *key)
                else:
                    logger.debug('orphan chunk for UUB %d, port %d, id %08x',
                                 *key)
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
        pos = len(filter(lambda i: start >= i, self.ends))
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
        headerdict = dict(zip(self.HEADER,
                              unpack('<%dL' % len(self.HEADER), header)))
        self.__dict__.update(headerdict)
        self.id &= 0x7FFFFFFF
        self.uubnum = uubnum
        self.details = details
        self.rawdata = bytearray(self.RAWDATASIZE)
        self.yall = None
        self.cover = Coverage(self.RAWDATASIZE)

    def addChunk(self, chunk):
        """Add a chunk into data. Return True if data complete."""
        # fragment header
        cid, start, end = unpack('<LHH', chunk[:self.FRAGHEADLEN])
        if len(chunk) - self.FRAGHEADLEN != end - start:
            raise ValueError("Wrong start/end versus chunk length")
        if cid != self.id:
            raise ValueError("Wrong id %d (%d expected)" % (cid, self.id))
        if not self.cover.insert(start, end):
            raise ValueError("Incompatible chunk (%d, %d), already covered %s"
                             % (start, end, self.cover.__str__()))
        self.rawdata[start:end] = bytearray(chunk[self.FRAGHEADLEN:])
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
        yall = numpy.zeros([self.NPOINT, 10], dtype=float)
        start = self.shwr_buf_start
        for i in xrange(self.NPOINT):
            index = (i + start) % self.NPOINT
            for j in xrange(5):
                off = (j*self.NPOINT+index)*4
                hg, lg = unpack("<HH", self.rawdata[off:off+4])
                yall[i, 2*j] = hg & 0xFFF
                yall[i, 2*j+1] = lg & 0xFFF
        self.yall = yall
        return yall


class UUBconvData(threading.Thread):
    """Thread to convert UUB rawdata to numpy"""
    stop = threading.Event()
    timeout = 1.0

    def __init__(self, q_ndata, q_dp):
        """Constructor.
q_ndata - a queue to listen for NetscopeData
q_dp - a queue to send numpy data
"""
        super(UUBconvData, self).__init__()
        self.q_ndata, self.q_dp = q_ndata, q_dp

    def run(self):
        logger = logging.getLogger('UUBconvData')
        while not self.stop.is_set():
            try:
                nd = self.q_ndata.get(True, self.timeout)
            except Empty:
                continue
            logger.debug('processing UUB %04d, id %08x start',
                         nd.uubnum, nd.id)
            flags = nd.details.copy() if nd.details is not None else {}
            flags['uubnum'] = nd.uubnum
            flags['yall'] = nd.convertData()
            self.q_dp.put(flags)
            logger.debug('processing UUB %04d, id %08x done', nd.uubnum, nd.id)
        logger.info("Leaving run()")


class UUBtelnet(threading.Thread):
    """Class making telnet to UUBs and run netscope program"""
    CMDS = ("tftp -g -r netscope.elf -l netscope 192.168.31.254",
            "chmod +x netscope",
            "./netscope >&/dev/null")
    LOGIN = "root"
    PASSWD = "root"

    def __init__(self, timer, *uubnums):
        super(UUBtelnet, self).__init__()
        self.timer = timer
        self.uubnums = list(uubnums)
        self.telnets = [Telnet() for uubnum in self.uubnums]
        self.uubnums2add = []
        self.uubnums2del = []
        self.logger = logging.getLogger('UUBtelnet')

    def login(self, uubnums=None):
        """Login to UUBs
uubnums - if not None, logs in only to these UUB"""
        for uubnum, tn in zip(self.uubnums, self.telnets):
            if uubnums is not None and uubnum not in uubnums:
                continue
            self.logger.debug('logging to UUB %04d', uubnum)
            tn.open(uubnum2ip(uubnum))
            tn.read_until("login: ")
            tn.write(UUBtelnet.LOGIN + "\n")
            tn.read_until("Password: ")
            tn.write(UUBtelnet.PASSWD + "\n")
            for cmd in UUBtelnet.CMDS:
                tn.write(cmd + "\n")
                tn.read_until(cmd + "\r\n")

    def logout(self, uubnums=None):
        """Logout from UUBs
uubnums - if not None, logs out only from these UUB"""
        for uubnum, tn in zip(self.uubnums, self.telnets):
            if uubnums is not None and uubnum not in uubnums:
                continue
            self.logger.debug('logging out of UUB %04d', uubnum)
            tn.close()

    def run(self):
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                self.logger.info('Timer stopped, closing telnets')
                self.logout()
                return
            # timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            while self.uubnums2add:
                uubnum = self.uubnums2add.pop()
                if uubnum in self.uubnums:
                    continue
                self.uubnums.append(uubnum)
                self.telnets.append(Telnet())
            while self.uubnums2del:
                uubnum = self.uubnums2del.pop()
                if uubnum not in self.uubnums:
                    continue
                ind = self.uubnums.index(uubnum)
                tn = self.telnets.pop(ind)
                tn.close()
                self.uubnums.pop(ind)
            if 'power.logout' in flags:
                self.logger.info('logout event')
                self.logout(flags['power.logout'])
            if 'power.login' in flags:
                self.logger.info('login event')
                self.login(flags['power.login'])
