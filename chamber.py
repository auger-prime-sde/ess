#
# ESS procedure - communication

from datetime import datetime
import threading
import logging

from modbus import Modbus, ModbusError, Binder, BinderSegment, BinderProg

class Chamber(threading.Thread):
    """Thread managing Climate chamber"""

    def __init__(self, port, timer, q_resp):
        """Constructor.
port - serial port to connect
timer - instance of Timer
q_resp - queue to send response"""
        super(Chamber, self).__init__()
        self.timer = timer
        self.q_resp = q_resp
        # check that we are connected to Binder climate chamber
        logger = logging.getLogger('chamber')
        b = None
        try:
            m = Modbus(port)
            logger.info('Opening serial %s' % repr(m.ser))
            b = Binder(m)
            b.state()
        except ModbusError:
            m.ser.close()
            logger.exception('Init modbus failed')
            raise
        self.binder = b

    def run(self):
        logger = logging.getLogger('chamber')
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                logger.info('Timer stopped, closing serial')
                self.binder.modbus.ser.close()
                return
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags
            logger.debug('Chamber event timestamp '
                         + datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S"))
            if 'binder.progstart' in flags:
                progno = int(flags['binder.progstart'])
                logger.info('Starting program %d' % progno)
                self.binder.setState(Binder.STATE_PROG, progno)
            if 'binder.progstop' in flags:
                logger.info('Stopping program')
                self.binder.setState(Binder.STATE_BASIC)
            if 'meas.thp' in flags or 'meas.point' in flags:
                logger.debug('Chamber temperature & humidity measurement')
                temperature = self.binder.getActTemp()
                humid = self.binder.getActHumid()
                logger.debug('Done. t = %.2fdeg.C, h = %.2f%%' % (temperature, humid))
                res = {'timestamp': timestamp,
                       'chamber.temp': temperature,
                       'chamber.humid': humid }
                if 'meas.point' in flags:
                    res['meas_point'] = flags['meas.point']
                self.q_resp.put(res)
