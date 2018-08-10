"""

 ESS procedure
 communication with UUB to get Zynq temperature and Slowcontrol data
 Implementation of UUB dispatcher & UUB meas
"""

import httplib
import json
import logging
import pickle
import re
import select
import socket
import threading
from datetime import datetime, timedelta
from time import sleep
from struct import pack, unpack
from binascii import hexlify
import numpy

PORT = 80

MSGLEN_DISP = 9    # length of message from dispatcher to meas
MSGLEN_MEAS = 11   # length of message from meas to dispatcher
TOUT_ACK = 1.0     # timeout for ack
TOUT_DONE = 5.0    # timeout for done

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

def hashObj(o):
    """Hash serialized object to 8B string"""
    return pack(">q", hash(pickle.dumps(o)))

def gener_param_default(**kwargs):
    """Measurement parameters generator: return provided kwargs as dict"""
    yield kwargs
    return

def gener_voltage_ch2(default_voltage, default_ch2):
    """Return generator of voltage & ch2.
default_voltage - a default voltage to be returned if not provided
                - list of floats
default_ch2 - default values of ch2 (list of 'ON'/'OFF' or True/False)
"""
    def gener(voltage=None, ch2=None, **kwargs):
        if voltage is None:
            voltage = default_voltage
        if ch2 is None:
            ch2 = default_ch2
        d = kwargs.copy()
        for ch in ch2:
            d['ch2'] = ch
            for v in voltage:
                d['voltage'] = v
                yield d
    return gener

def isLive(uub, timeout=0):
    """Try open TCP to UUB:80, eventually repeat until timeout expires.
Return True if UUB answers, False if timeout occurs.
uub - UUBmeas or UUBtsc (must have ip and logger attributes)
"""
    # uub.logger.debug('isLive(%f)', timeout)
    exptime = datetime.now() + timedelta(seconds=timeout)
    # uub.logger.debug('exptime = %s', exptime.strftime("%Y-%m-%d %H:%M:%S,%f"))
    addr = (uub.ip, PORT)
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
    """Thread managing read out Zynq temperature and SlowControl data from UUB"""

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
   N3V3  \s+ (?P<u_N3V3>\d+(\.\d+)?) \s* \[mV\] \s*
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
            if (not 'meas.thp' in flags and 'meas.sc' not in flags
                and 'power.test' in flags):
                continue
            res = {'timestamp': timestamp}
            if 'power.test' in flags:
                res['live%04d' % self.uubnum] = isLive(self)
                if 'test_point' in flags:
                    res['test_point'] = flags['test_point']
            self.logger.debug('Connecting UUB')
            conn = httplib.HTTPConnection(self.ip, PORT)
            try:
                # read Zynq temperature
                if 'meas.thp' in flags:
                    res.update(self.readZynqTemp(conn))
                # read SlowControl data
                if 'meas.sc' in flags:
                    res.update(self.readSlowControl(conn))
            except (httplib.CannotSendRequest, socket.error, AttributeError) as e:
                self.logger.error('HTTP request failed, %s', e.__str__())
            finally:
                conn.close()
                self.logger.debug('HTTP connection closed')
            self.q_resp.put(res)

    def readSerialNum(self, timeout=None):
        """Read UUB serial number
Return as 'ab-cd-ef-01-00-00' or None if UUB is not live"""
        re_sernum = re.compile(r'.*\nSN: (?P<sernum>' +
                               r'([a-fA-F0-9]{2}-){5}[a-fA-F0-9]{2})', re.DOTALL)

        if timeout is not None and not isLive(self, timeout):
            return None
        self.logger.debug('Reading UUB serial number')
        conn = httplib.HTTPConnection(self.ip, PORT)
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
        re_zynqtemp = re.compile(r'Zynq temperature: (?P<zt>[+-]?\d+(\.\d*)?)' +
                                 r' degrees')
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

class UUBdisp(threading.Thread):
    """UUB dispatcher"""
    def __init__(self, timer, afg, gener_param=gener_param_default):
        """Constructor
timer - instance of timer
afg - instance of AFG
gener_param - generator of measurement paramters
"""
        super(UUBdisp, self).__init__()
        self.timer, self.afg, self.gener_param = timer, afg, gener_param
        self.socks = []
        self.socks2add = []
        self.flags = {}  # current task paramterers
        self.h = None    # hash of current task

    def registerUUB(self, uubnum):
        """Create a socket pair for UUB, store one socket in UUBdisp and
return the second to UUBmeas.
uubnum currently not stored
"""
        sock_disp, sock_meas = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_DGRAM)
        self.socks2add.append(sock_disp)
        return sock_meas

    def setParams(self, **params):
        """Perform operations with set-up (voltage, function generator etc.)
according to params.
params keys: voltage, ch2
"""
        logger = logging.getLogger('UUBdisp')
        logger.debug('setParams %s', repr(params))
        kw = {k: params[k] for k in ('voltage', 'ch2')
              if k in params}
        self.afg.setOn(**kw)

    def clearParams(self):
        """Move experiment to idle after all parameters"""
        logger = logging.getLogger('UUBdisp')
        logger.debug('clearParams')
        self.afg.setOff()

    def checkSocks(self, socks, tout, msg):
        """Wait for msg on all socks or timeout
socks - list of sockets to wait for message (will be modified)
tout - timeout
msg - expected message prefix
"""
        tend = datetime.now() + timedelta(seconds=tout)
        socklist = []
        while datetime.now() < tend and socks:
            tout = (tend - datetime.now()).total_seconds()
            rlist, wlist, xlist = select.select(socks, [], [], tout)
            for sock in rlist:
                resp = sock.recv(MSGLEN_MEAS)
                if resp[:MSGLEN_DISP] == msg:
                    socklist.append(sock)
                    socks.remove(sock)
        return socklist

    def run(self):
        logger = logging.getLogger('UUBdisp')
        while True:
            self.timer.evt.wait()
            # move socks2add -> socks
            while self.socks2add:
                self.socks.append(self.socks2add.pop())
            if self.timer.stop.is_set():
                logger.info('Timer stopped, stopping UUB meas')
                for s in self.socks:
                    s.send('K' + '\xFF'*8)
                return
            if 'meas.point' not in self.timer.flags:
                logger.debug('Ticker meas.point not in timer.flags, skipping')
                continue
            timestamp = self.timer.timestamp   # store info from timer
            tflags = self.timer.flags['meas.point']
            logger.debug('Event timestamp %s, flags: %s',
                         datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S"),
                         repr(tflags))
            # copy other relevant flags
            aflags = {flag: tflags[flag] for flag in ('db_point', 'set_temp', 'meas_point')
                          if flag in tflags}
            aflags['timestamp'] = timestamp
            # run measurement for all parameters
            for flags in self.gener_param(**tflags):
                self.setParams(**flags)
                flags.update(aflags)
                h = hashObj(flags)
                self.h, self.flags = h, flags
                # send measurement request
                msg = 'M' + h
                for s in self.socks:
                    s.send(msg)
                socks2wait = self.checkSocks(self.socks[:], TOUT_ACK, 'A'+h)
                self.checkSocks(socks2wait, TOUT_DONE, 'D'+h)
                # send cancel request
                msg = 'C' + h
                for s in self.socks:
                    s.send(msg)
            self.clearParams()

class UUBmeas(threading.Thread):
    """Implementation of thread for UUB data readout"""
    def __init__(self, uubnum, disp, q_dp):
        """Constructor.
uubnum - UUB number
disp - an instance of UUBdisp (to get sock_meas & flags)
q_dp - a queue for results
"""
        super(UUBmeas, self).__init__()
        self.uubnum, self.disp, self.q_dp = uubnum, disp, q_dp
        self.sock = disp.registerUUB(uubnum)
        self.ip = uubnum2ip(uubnum)
        self.logger = logging.getLogger('UUBm%04d' % uubnum)
        self.logger.info('UUB meas created, IP %s.', self.ip)

    def checkSock(self, timeout=None):
        """Check if dgram arrived to sock
return (None, None) nothing received or K not for us or wrong msg
       ('K', None) to stop
       ('M', flaghash) to start measurement
       ('C', flaghash) to cancel measurement
"""
        rlist, wlist, xlist = select.select([self.sock], [], [], timeout)
        if not rlist:
            return None, None  # nothing received - timeout
        msg = self.sock.recv(MSGLEN_DISP)
        if len(msg) < MSGLEN_DISP:
            self.logger.error('Incomplete msg <%s>', repr(msg))
            return None, None
        if msg[0] == 'K':   # stop if K is for us (=uubnum) or for all (FFFF)
            uubnum = unpack(">H", msg[1:3])[0]
            if uubnum in (self.uubnum, 0xFFFF):
                self.logger.info('Kill received')
                return 'K', None
        elif msg[0] in ('M', 'C'):
            return msg[0], msg[1:]
        else:
            self.logger.error('Wrong msg <%s>', repr(msg))
            return None, None

    def run(self):
        while True:
            # IDLE state, wait for message from dispatcher
            cmd, flaghash = self.checkSock()
            if cmd == 'K':
                return
            elif cmd == 'C':
                self.logger.debug('Cancel %s', hexlify(flaghash))
                continue
            elif cmd != 'M':
                self.logger.error('Msg type M expected')
                continue
            # transit to WORK state
            flags = self.disp.flags.copy()
            self.logger.debug('Meas %s', hexlify(flaghash))
            self.logger.debug('flags %s', repr(flags))
            # send Ack to dispatcher
            self.sock.send('A' + flaghash)
            self.logger.debug('Ack %s sent', hexlify(flaghash))
            # send request
            conn = httplib.HTTPConnection(self.ip, PORT)
            try:
                conn.request('GET', '/cgi-bin/getdata.cgi?action=scope')
            except (httplib.CannotSendRequest, socket.error, AttributeError) as e:
                self.logger.error('HTTP request failed, %s', e.__str__())
                conn.close()
            self.logger.debug('Request scope sent')
            cmd, flaghash1 = self.checkSock(0)
            if cmd is not None:
                conn.close()
                if cmd == 'K':
                    self.logger.info('Kill received')
                    break
                elif cmd == 'C':
                    self.logger.info('Canceling measurement %s',
                                     hexlify(flaghash))
                    if flaghash != flaghash1:
                        self.logger.error('Unexpected flaghash %s',
                                          hexlify(flaghash1))
                    continue
                elif cmd == 'M':
                    self.logger.error('Unexpected measurement msg')
                    continue
            # wait for response
            try:
                resp = conn.getresponse()
            except (httplib.CannotSendRequest, socket.error, AttributeError) as e:
                self.logger.error('HTTP get response failed, %s', e.__str__())
                conn.close()
                continue
            # send Done to dispatcher
            self.sock.send('D' + flaghash)
            self.logger.debug('Done %s sent', hexlify(flaghash))
            conn.close()
            self.logger.debug('Response %d received', resp.status)
            # TO DO: retry for not OK response
            if resp.status != httplib.OK:
                continue
            # process data and pass to data processor
            labels = ['adc%d' % i for i in range(10)]
            data = resp.read()
            self.logger.debug('Response read')
            # transform json data to list(2048) of lists(10)
            try:
                y = [[float(trec[label]) for label in labels]
                     for trec in json.loads(data)]
                self.logger.debug('JSON transformed to list')
            except ValueError as e:
                self.logger.error('Error parsing UUB data, %s', e.__str__())
                continue 
            flags['yall'] = numpy.array(y, dtype='float32')
            flags['uubnum'] = self.uubnum
            self.q_dp.put(flags)
            self.logger.debug('Data put to q_dp')
