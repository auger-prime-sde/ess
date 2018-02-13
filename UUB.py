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
        for v in voltage:
            d['voltage'] = v
            for ch in ch2:
                d['ch2'] = ch
                yield d
    return gener

class UUBtsc(threading.Thread):
    """Thread managing read out Zynq temperature and SlowControl data from UUB"""

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
        self.logger = logging.getLogger('UUB-%04d' % uubnum)
        self.logger.info('UUBtsc created.')

    def run(self):
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                self.logger.info('UUBtsc stopped')
                return
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            if not 'meas.thp' in flags and 'meas.sc' not in flags:
                continue
            res = {'timestamp': timestamp}
            self.logger.debug('Connecting UUB')
            conn = httplib.HTTPConnection(self.ip, PORT)
            # read Zynq temperature
            if 'meas.thp' in flags:
                res.update(self.readZynqTemp(conn))
            # read SlowControl data
            if 'meas.sc' in flags:
                res.update(self.readSlowControl(conn))
            conn.close()
            self.logger.debug('HTTP connection closed')
            self.q_resp.put(res)

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
return dictionary: sc<uubnum>_<variable>: temperature
"""
        re_scdata = re.compile(r'Zynq temperature: (?P<zt>[+-]?\d+(\.\d*)?)' +
                               r' degrees')
        conn.request('GET', '/cgi-bin/getdata.cgi?action=slowc&arg1=-a')
        # TO DO: check status
        resp = conn.getresponse().read()
        self.logger.debug('slowc GET: "%s"', resp)
        m = re_scdata.match(resp)
        res = {}
        if m is not None:
            # prefix keys
            for k, v in m.groupdict().iteritems():
                res['sc%04d_%s' % (self.uubnum, k)] = float(v)
        else:
            self.logger.warning('Resp to slowc does not match Zynq temperature')
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
            # run measurement for all parameters
            for flags in self.gener_param(**tflags):
                logger.debug('flags %s', repr(flags))
                self.setParams(**flags)
                flags['timestamp'] = timestamp
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
        self.logger = logging.getLogger('UUBm%04d' % uubnum)
        self.logger.info('UUB meas created')

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
        ip = uubnum2ip(self.uubnum)
        self.logger.debug('Run, IP = %s', ip)
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
            conn = httplib.HTTPConnection(ip, PORT)
            conn.request('GET', '/cgi-bin/getdata.cgi?action=scope')
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
            resp = conn.getresponse()
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
            y = [[float(trec[label]) for label in labels]
                 for trec in json.loads(data)]
            self.logger.debug('JSON transformed to list')
            flags['yall'] = numpy.array(y, dtype='float32')
            flags['uubnum'] = self.uubnum
            self.q_dp.put(flags)
            self.logger.debug('Data put to q_dp')
