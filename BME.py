#
# ESS procedure
# communication with BME280 on arduino
#

import re
import threading
import logging
from time import sleep
from datetime import datetime, timedelta
from serial import Serial


class SerialReadTimeout(AssertionError):
    pass


def readSerRE(ser, r, timeout=2, logger=None):
    """Try to read regexp 're' from serial with timeout
r - compiled regexp
timeout - timeout in s after which SerialReadTimeout exception is raised
return response from serial or raise SerialReadTimeout exception"""
    TIME_STEP = 0.01   # timestep between successive read trials
    resp = ser.read(ser.inWaiting())
    if r.match(resp):
        if logger is not None:
            logger.debug("serial %s read %s" % (ser.port, repr(resp)))
        return resp
    tend = datetime.now() + timedelta(seconds=timeout)
    while datetime.now() < tend:
        if ser.inWaiting() > 0:
            resp += ser.read(ser.inWaiting())
            if r.match(resp):
                if logger is not None:
                    logger.debug("serial %s read %s" % (ser.port, repr(resp)))
                return resp
        sleep(TIME_STEP)
    if logger is not None:
        logger.debug("serial %s timed out, partial read %s" % (
            ser.port, repr(resp)))
    raise SerialReadTimeout


class BME(threading.Thread):
    """Thread managing arduino reading BME280"""
    re_bmeinit = re.compile(r'.*BME1 detected[\r\n]*BME2 detected[\r\n]*',
                            re.DOTALL)
    re_bmetimeset = re.compile(r'.*set time OK[\r\n]*', re.DOTALL)
    re_bmemeas = re.compile(r'.*(?P<dt>20\d{2}-\d{2}-\d{2}T' +
                            r'\d{2}:\d{2}:\d{2})' +
                            r' +(?P<temp1>-?\d+(\.\d*)?).*' +
                            r' +(?P<humid1>\d+(\.\d*)?).*' +
                            r' +(?P<press1>\d+(\.\d*)?).*' +
                            r' +(?P<temp2>-?\d+(\.\d*)?).*' +
                            r' +(?P<humid2>\d+(\.\d*)?).*' +
                            r' +(?P<press2>\d+(\.\d*)?)[\r\n]*',
                            re.DOTALL)

    def __init__(self, port, timer, q_resp, timesync=False):
        """Constructor.
port - serial port to connect
timer - instance of timer
q_resp - queue to send response
timesync - sync Arduino time
"""
        super(BME, self).__init__()
        self.timer = timer
        self.q_resp = q_resp
        # check that we are connected to BME
        logger = logging.getLogger('bme')
        s = None               # avoid NameError on isinstance(s, Serial) check
        try:
            s = Serial(port, baudrate=115200)
            logger.info('Opening serial %s', repr(s))
            resp = readSerRE(s, BME.re_bmeinit, timeout=3, logger=logger)
            if timesync:
                # initialize time
                logger.info('BME time sync')
                ts = (datetime.now() + timedelta(seconds=1)).strftime(
                    "t %Y-%m-%dT%H:%M:%S\r")
                s.write(ts)
                resp = readSerRE(s, BME.re_bmetimeset,
                                 timeout=3, logger=logger)
                logger.info('synced to ' + ts)
        except Exception:
            logger.exception("Init serial with BME failed")
            if isinstance(s, Serial):
                logger.info('Closing serial %s', s.port)
                s.close()
            raise SerialReadTimeout
        self.ser = s

    def __del__(self):
        logger = logging.getLogger('bme')
        logger.info('Closing serial')
        try:
            self.ser.close()
        except Exception:
            pass

    def run(self):
        logger = logging.getLogger('bme')
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                logger.info('Timer stopped, closing serial')
                self.ser.close()
                return
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            if any([name in flags
                    for name in ('meas.thp', 'meas.pulse', 'meas.freq')]):
                logger.debug('BME event timestamp ' +
                             datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S"))
                logger.debug('BME read')
                self.ser.write('m')
                resp = readSerRE(self.ser, BME.re_bmemeas, logger=logger)
                logger.debug('BME read finished')
                d = BME.re_bmemeas.match(resp).groupdict()
                bmetime = d.pop('dt')
                logger.debug('BME vs event time diff: %f s',
                             (datetime.strptime(bmetime, '%Y-%m-%dT%H:%M:%S') -
                              timestamp).total_seconds())
                res = {'timestamp': timestamp}
                # prefix keys from re_bme with 'bme.'
                for k, v in d.iteritems():
                    res['bme_'+k] = float(v)
                self.q_resp.put(res)


class TrigDelay(object):
    """Interface to arduino managing trigger delay"""
    re_init = re.compile(r'.*TrigDelay (?P<version>\d+)', re.DOTALL)
    re_setdelay = re.compile(r'.*OK', re.DOTALL)
    re_getdelay = re.compile(r'.*trigdelay .*: (?P<delay>\d+)', re.DOTALL)

    def __init__(self, port, predefined=None):
        """Constructor.
port - serial port to connect
predefined - dict functype: delay with predefined values """
        self.logger = logging.getLogger('TrigDelay')
        s = None               # avoid NameError on isinstance(s, Serial) check
        try:
            s = Serial(port, baudrate=115200)
            self.logger.info('Opening serial %s', repr(s))
            sleep(0.5)  # ad hoc constant to avoid timeout
            # s.write('?\r')
            resp = readSerRE(s, TrigDelay.re_init, timeout=1,
                             logger=self.logger)
            self.version = TrigDelay.re_init.match(resp).groupdict()['version']
        except Exception:
            self.logger.exception("Init serial with TrigDelay failed")
            if isinstance(s, Serial):
                self.logger.info('Closing serial %s', s.port)
                s.close()
            raise SerialReadTimeout
        self.ser = s
        self.predefined = predefined if predefined is not None else {}

    def __del__(self):
        self.ser.close()

    @property
    def delay(self):
        """Get delay from Arduino"""
        self.logger.info('getting delay')
        self.ser.write('q\r')
        resp = readSerRE(self.ser, TrigDelay.re_getdelay,
                         timeout=1, logger=self.logger)
        m = TrigDelay.re_getdelay.match(resp)
        return int(m.groupdict()['delay'])

    @delay.setter
    def delay(self, delay):
        """Set delay in <250ns> units"""
        ndelay = self.predefined.get(delay, delay)
        self.logger.info('setting delay %d * 3/16us', ndelay)
        self.ser.write('d %d\r' % ndelay)
        resp = readSerRE(self.ser, TrigDelay.re_setdelay,
                         timeout=1, logger=self.logger)
        self.logger.debug('delay set')
