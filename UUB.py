#
# ESS procedure
# communication with UUB to get Zynq temperature and Slowcontrol data
# 

import re
import threading
import logging
from time import sleep
from datetime import datetime, timedelta
import httplib, urllib

PORT = 8080

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
        self.ip = '192.168.%d.%d' % (31 + (uubnum >> 8), uubnum & 0xFF)
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
        self.logger.debug('xadc GET: "%s"' % resp)
        m = re_zynqtemp.match(resp)
        if m is not None:
            return {'zynq%04d_temp' % self.uubnum: float(m.groupdict()['zt'])}
        else:
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
        self.logger.debug('slowc GET: "%s"' % resp)
        m = re_scdata.match(resp)
        res = {}
        if m is not None:
            # prefix keys
            for k, v in m.groupdict().iteritems():
                res['sc%04d_%s' % (self.uubnum, k)] = float(v)
        else:
            self.logger.warning('Resp to slowc does not match Zynq temperature')
        return res
