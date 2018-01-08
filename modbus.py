#
# Python Modbus client on RS232/422
#
# Petr Tobiska <tobiska@fzu.cz>
#

VERSION = '20180104'

import sys
import time
from struct import pack, unpack
from binascii import hexlify, unhexlify
import logging
import serial
import crcmod

# constants
READ_HOLDING_REGISTERS = 0x03
WRITE_SINGLE_REGISTER = 0x06
WRITE_MULTIPLE_REGISTERS = 0x10

class ModbusError(Exception):
    ERROR_INVALID_FUN     = 1
    ERROR_INVALID_ADDR    = 2
    ERROR_PARAM_OUTSIDE   = 3
    ERROR_SLAVE_NOT_READY = 4
    ERROR_WRITE_DENIED    = 8

    def __init__(self, msg, code=None):
        super(ModbusError, self).__init__(msg)
        self.code = code
    def __str__(self):
        if self.code is not None:
            return ("Modbus error, code %d\n" % self.code)
        else:
            return super(ModbusError, self).__str__()

class Modbus:
    """Implementation of Modbus client subset"""
    def __init__(self, port, baudrate=9600, timeout=1, slave_id=1):
        """Constructor 
port      - device to connect (/dev/ optional), str
baudrate  - baudrate to use, int
timeout   - timeout for serial read (seconds, float)
"""
        self.slave_id = slave_id
        self.crc = crcmod.predefined.mkCrcFun('modbus')
        self.logger = logging.getLogger('modbus')
        if not port.startswith('/dev/') and sys.platform == 'linux2':
            port = '/dev/' + port
        try:
            self.ser = serial.Serial(port, baudrate, serial.EIGHTBITS,
                                     serial.PARITY_NONE, serial.STOPBITS_ONE,
                                     timeout=timeout)
        except serial.SerialException:
            raise ModbusError("Cannot open serial on port %s" % port)

    def __del__(self):
        self.ser.close()
        self.ser = None  # trigger serial.__del__() hopefully

    def send(self, data, n):
        """Append CRC to data and send through serial.
Receive response and check CRC.
data    - data to send (without CRC)
n       - expected size of response (without CRC)
return data with CRC stripped
"""
        data += pack('<H', self.crc(data))
        self.logger.debug(" => %s %s [%s]" % (
            hexlify(data[:2]), hexlify(data[2:-2]), hexlify(data[-2:])))
        nw = self.ser.write(data)
        if nw < len(data):
            self.logger.debug("written %d" % nw)
            raise ModbusError("Incomplete serial data write")
        # early detection of Modbus error
        resp = self.ser.read(5)
        if len(resp) < 5:
            self.logger.error("Incomplete serial read: <%s>" % hexlify(resp))
            raise ModbusError("Incomplete serial data read")
        if ord(resp[1]) & 0x80 and len(resp) == 5:
            self.logger.debug("<=  %02X %02X %02X [%04X]" %
                              unpack(">BBBH", resp))
            assert self.crc(resp) == 0, "Wrong CRC code"
            if resp[0] != data[0] or ord(resp[1]) & 0x7F != ord(data[1]):
                raise ModbusError("Malformed error response")
            raise ModbusError("Modbus error code", ord(resp[2]))
        if n > 3:   # read the rest of data if no error occurred
            resp += self.ser.read(n-3)
            self.logger.debug("<=  %s %s [%s]" % (
                hexlify(resp[:2]), hexlify(resp[2:-2]), hexlify(resp[-2:])))
        if self.ser.in_waiting > 0:
            self.ser.read(self.ser.in_waiting)  # empty read buffer
            raise ModbusError("Surplus data in serial read")
        if ord(resp[1]) & 0x80 and len(resp) == 5:
            assert self.crc(resp) == 0, "Wrong CRC code"
            if resp[0] != data[0] or ord(resp[1]) & 0x7F != ord(data[1]):
                raise ModbusError("Malformed error response")
            raise ModbusError("Modbus error code", ord(resp[2]))
        if len(resp) < n+2 or self.ser.in_waiting > 0:
            raise ModbusError("Incomplete serial data read")
        assert self.crc(resp) == 0, "Wrong CRC code"
        return resp[:-2]

    def read_holding_registers(self, reg_addr, reg_nb=1):
        """Modbus function READ_HOLDING_REGISTERS (0x03)
reg_addr     - register address (0 to 0xFFFF)
reg_nb       - number of registers (1 to 125)
""" 
        assert 0 <= reg_addr <= 0xFFFF, "reg_addr out of range"
        assert 1 <= reg_nb <= 80, "reg_nb out of range"
        frame = pack('>BBHH', self.slave_id, READ_HOLDING_REGISTERS,
                     reg_addr, reg_nb)
        resp = self.send(frame, 3 + 2*reg_nb)
        assert resp[:3] == pack('BBB', self.slave_id, READ_HOLDING_REGISTERS,
                                2*reg_nb), "wrong response header"
        return unpack('>'+'H'*reg_nb, resp[3:])

    def write_single_register(self, reg_addr, reg_value):
        """Modbus function WRITE_SINGLE_REGISTER (0x06)
reg_addr     - register address (0 to 0xFFFF)
reg_value    - value to write (0 to 0xFFFF)
""" 
        assert 0 <= reg_addr <= 0xFFFF, "reg_addr out of range"
        assert 0 <= reg_value <= 0xFFFF, "reg_value out of range"
        frame = pack('>BBHH', self.slave_id, WRITE_SINGLE_REGISTER,
                     reg_addr, reg_value)
        resp = self.send(frame, 6)
        assert resp == frame, "wrong response"

    def write_multiple_registers(self, reg_addr, reg_values):
        """Modbus function WRITE_MULTIPLE_REGISTERS (0x10)
reg_addr     - register address (0 to 0xFFFF)
reg_values   - list of values to write (each 0 to 0xFFFF)
""" 
        assert 0 <= reg_addr <= 0xFFFF, "reg_addr out of range"
        assert isinstance(reg_values, list), "reg_valus not a list"
        n = len(reg_values)
        assert 1 <= n <= 80, "wrong length of reg_values list"
        for v in reg_values:
            assert 0 <= v <= 0xFFFF, "wrong word value"
        frame = pack('>BBHHB'+'H'*n, self.slave_id, WRITE_MULTIPLE_REGISTERS,
                     reg_addr, n, 2*n, *reg_values)
        resp = self.send(frame, 6)
        assert resp == frame[:6], "wrong response"

    def read_float(self, reg_addr):
        """Read 2 words from <reg_addr> and convert them to float"""
        return words2floats(*self.read_holding_registers(reg_addr, 2))[0]

    def write_float(self, reg_addr, fval):
        """Convert float to 2 words and write them to <reg_addr>"""
        self.write_multiple_registers(reg_addr, floats2words(fval))

    def write_int(self, reg_addr, ival):
        """Convert int to 2 words and write them to <reg_addr>"""
        self.write_multiple_registers(reg_addr, [ival / 0x10000, ival % 0x10000])
        
def words2floats(*words):
    """Convert list of 2*n words to list of n floats"""
    assert len(words) % 2 == 0, "Even number of words expected"
    it = iter(words)
    vals = []
    for w1 in it:
        blob = pack(">HH", next(it), w1)
        vals.append(unpack(">f", blob)[0])
    return vals

def floats2words(*floats):
    """Convert list of n floats to list of 2*n words"""
    vals = []
    for f in floats:
        blob = pack(">f", f)
        w1, w2 = unpack(">HH", blob)
        vals.extend([w2, w1])
    return vals

class Binder:
    """Interface to Binder MKFT 115"""
    # register addresses
    ADDR_ACT_TEMP      = 0x11A9
    ADDR_ACT_HUMID     = 0x11CD
    ADDR_SET_TEMP      = 0x1177
    ADDR_SET_HUMID     = 0x1179
    ADDR_SET_TEMP_BAS  = 0x1581
    ADDR_SET_HUMID_BAS = 0x1583
    ADDR_SET_TEMP_MAN  = 0x156F
    ADDR_SET_HUMID_MAN = 0x1571
    ADDR_PROG_NO       = 0x1A02            # program number
    ADDR_PROG_TYPE     = 0x1A03            # 0 .. temperature, 1 .. humidity
    ADDR_PROG_SEG      = 0x1A04            # segment programmed
    ADDR_PROG_NSEG     = 0x1A05            # total number of segments
    ADDR_PROG_6        = 0x1A06            # ??? repetion ?
    ADDR_PROG_VAL      = 0x1A07            # target value 
    ADDR_PROG_GRAD     = 0x1A09            # gradient ?
    ADDR_PROG_LIMI     = 0x1A0B            # limit min
    ADDR_PROG_LIMA     = 0x1A0D            # limit max
    ADDR_PROG_DUR      = 0x1A0F            # duration
    ADDR_PROG_OPERC    = 0x1A11            # operational contacts
    ADDR_PROG_12       = 0x1A12            # ??? repetion ?
    ADDR_PROG_13       = 0x1A13            # ??? repetion ?
    ADDR_PROG_END      = 0x1599            # end of program
    
    ADDR_MODE          = 0x1A22
    ADDR_PROGNO        = 0x1A23
    # constants
    STATE_BASIC =  0x1000
    STATE_MANUAL = 0x0800
    STATE_PROG =   0x0400
    P_TEMP = 0
    P_HUMID = 1

    def __init__(self, modbus):
        assert isinstance(modbus, Modbus), "Modbus instance expected"
        self.modbus = modbus

    def reset(self):
        """Some initialization"""
        self.modbus.write_single_register(0x1A00, 5)
        v1 = self.modbus.read_holding_registers(0x1A00)
        v2 = self.modbus.read_holding_registers(0x1A01)

    def state(self):
        """Read state from chamber"""
        return self.modbus.read_holding_registers(Binder.ADDR_MODE)

    def setState(self, state, progno=None):
        assert state in (Binder.STATE_BASIC, Binder.STATE_MANUAL,
                         Binder.STATE_PROG), "Incorrect Binder state"
        if state == Binder.STATE_PROG and progno is not None:
            assert 0 <= progno <= 19, "Incorrect Prog No"
        s = self.state()
        self.modbus.write_single_register(Binder.ADDR_MODE, 0)
        if state == Binder.STATE_PROG and progno is not None:
            self.modbus.write_single_register(Binder.ADDR_PROGNO, progno)
        self.modbus.write_single_register(Binder.ADDR_MODE, state)

    def getActTemp(self):
        return self.modbus.read_float(Binder.ADDR_ACT_TEMP)

    def getActHumid(self):
        return self.modbus.read_float(Binder.ADDR_ACT_HUMID)

class BinderSegment:
    """Program segment"""
    def __init__(self, val, duration, **kw):
        self.val = val
        self.duration = duration
        self.grad = kw.get('grad', 200004.)
        self.minlim = kw.get('minlim', -999.)
        self.maxlim = kw.get('maxlim', 999.)
        self.operc = kw.get('operc', 0)


class BinderProg:
    """Implementation of Program for Binder MKFT 115"""
    def __init__(self):
        self.seg_temp = []
        self.seg_humid = []

    def lengths(self):
        """Calculates lengths of temperature/humidity segments"""
        def fs(x, seg):
            return x + seg.duration
        ltemp = reduce(fs, self.seg_temp, 0.0)
        lhumid = reduce(fs, self.seg_humid, 0.0)
        return ltemp, lhumid

    def send(self, binder, progno):
        assert isinstance(binder, Binder), "Binder instance expected"
        assert 0 <= progno <= 100, "Wrong Prog Number"
        m = binder.modbus
        binder.reset()
        for segtype, segs in ((Binder.P_TEMP, self.seg_temp),
                              (Binder.P_HUMID, self.seg_humid)):
            m.write_single_register(Binder.ADDR_PROG_NO, progno)
            m.write_single_register(Binder.ADDR_PROG_TYPE, segtype)
            m.write_single_register(Binder.ADDR_PROG_NSEG, len(segs))

            for i, s in enumerate(iter(segs)):
                m.write_single_register(Binder.ADDR_PROG_SEG, i)
                m.write_single_register(Binder.ADDR_PROG_6, 0)
                m.write_float(Binder.ADDR_PROG_VAL, s.val)
                m.write_float(Binder.ADDR_PROG_GRAD, s.grad)
                m.write_int(Binder.ADDR_PROG_DUR, s.duration)
                m.write_single_register(Binder.ADDR_PROG_12, 0)
                m.write_single_register(Binder.ADDR_PROG_13, 0)
                m.write_float(Binder.ADDR_PROG_LIMI, s.minlim)
                m.write_float(Binder.ADDR_PROG_LIMA, s.maxlim)
                if segtype == Binder.P_TEMP:
                    m.write_single_register(Binder.ADDR_PROG_OPERC, s.operc)
                binder.reset()
        m.write_single_register(Binder.ADDR_PROG_END, 0)
                    
                
            
