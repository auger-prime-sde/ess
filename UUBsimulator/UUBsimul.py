"""
 Simulator of UUB
 HTTP server answering some GET requests like UUB
"""

import os
import re
import logging
import subprocess
import threading
import BaseHTTPServer
from time import sleep
import urllib

PORT = 80

# path to data returned by scope
SCOPEDATAFN = os.path.dirname(__file__) + '/scopedata'

# manage running httpd in separates threads
run = True
def keep_running():
    return run

def uubnum2ip(uubnum):
    """Calculate IP address from UUB number"""
    return '192.168.%d.%d' % (16 + (uubnum >> 8), uubnum & 0xFF)

def MakeHandler(zt, sc, data):
    """Make paremetrized handler"""
    class UubHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler, object):
        def __init__(self, *args, **kwargs):
            """Constructor.
zt - generator providing Zynq temperature
sc - generator providing values measured by Slow Control
data - generator providing (10*2048) data from ADC"""
            self.zt, self.sc, self.data = zt, sc, data
            super(UubHttpHandler, self).__init__(*args, **kwargs)

        def do_GET(self):
            re_path = re.compile(r'^/cgi-bin/getdata.cgi\?' +
                                 r'action=(?P<action>[^&]+)' +
                                 r'(&arg1=(?P<arg1>[^&]+))?' +
                                 r'(&arg2=(?P<arg2>[^&]+))?')
            m = re_path.match(self.path)
            action = m.groupdict()['action'] if m is not None else None
            if action not in ('slowc', 'xadc', 'scope'):
                self.send_response(404)
                self.end_headers()
                return
            if action == 'xadc':
                response = 'Zynq temperature: %.1f degrees\n' % self.zt.next()
            elif action == 'slowc':
                response = 'Slow control output'
            else:  # scope
                response = self.data.next()
            self.send_response(200)
            self.send_header("Content-type", "text/plain;charset=us-ascii")
            self.end_headers()
            self.wfile.write(response)

    return UubHttpHandler

class UUBsimul(threading.Thread):
    """Simulator of UUB: http server answering some GET requests"""

    def __init__(self, uubnum, zt, sc, data):
        """Constructor.
uubnum - UUB number
zt - generator providing Zynq temperature
sc - generator providing values measured by Slow Control
data - generator providing (10*2048) data from ADC
"""
        super(UUBsimul, self).__init__()
        ip = uubnum2ip(uubnum)
        self.uubnum = uubnum
        self.logger = logging.getLogger('UUB #%d' % self.uubnum)
        self.logger.info('Creating UUB %d on ip %s', uubnum, ip)
        server_class = BaseHTTPServer.HTTPServer
        handler = MakeHandler(zt, sc, data)
        self.httpd = server_class((ip, PORT), handler)

    def run(self):
        self.logger.info('Starting server')
        while keep_running():
            self.httpd.handle_request()
        self.logger.info('Run finished')

    def __del__(self):
        self.logger.info('Stopping server')

def zt_gen(zynq_temp):
    while True:
        try:
            with open('/sys/class/thermal/thermal_zone0/temp', 'r') as f:
                t = float(f.read())/1000.
        except IOError:
            t = zynq_temp
        yield t

# placeholder
def sc_gen():
    yield None

def data_gen():
    with open(SCOPEDATAFN, 'r') as f:
        data = f.read()
    while True:
        yield data

def findUUBnums():
    """Check network config and identify UUB interfaces"""
    re_inet = re.compile(r'^ *inet 192\.168\.(\d+)\.(\d+) ')
    uubnums = []
    for line in subprocess.check_output(['/sbin/ifconfig']).splitlines():
        m = re_inet.match(line)
        if m is None:
            continue
        x, y = map(int, m.groups())
        if x >> 4 == 1:
            uubnums.append(((x & 0xF) << 8) + y)
    return uubnums

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    servers = {}
    for uubnum in findUUBnums():
        servers[uubnum] = UUBsimul(uubnum, zt_gen(47.9), sc_gen(), data_gen())

    for server in servers.itervalues():
        server.start()

    logger = logging.getLogger('UUB simul')
    logger.info('Servers started, press ^C to stop')
    while True:
        try:
            sleep(1)
        except (KeyboardInterrupt, SystemExit):
            # shutdown servers: they wait for http req
            run = False
            for uubnum in servers.iterkeys():
                url = 'http://%s/' % uubnum2ip(uubnum)
                urllib.urlopen(url)
            break
