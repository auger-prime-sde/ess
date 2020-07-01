#
# Python Modbus client on RS232/422
#
# Petr Tobiska <tobiska@fzu.cz>
#

from struct import pack, unpack
import logging
import serial
import crcmod

VERSION = '20200618'

# constants
READ_HOLDING_REGISTERS = 0x03
READ_INPUT_REGISTERS = 0x04
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
    def __init__(self, port, baudrate=9600, timeout=1, slave_id=1, echo=False):
        """Constructor
port      - device to connect, str
baudrate  - baudrate to use, int
timeout   - timeout for serial read (seconds, float)
echo      - sent data are echoed
"""
        self.slave_id = slave_id
        self.echo = echo
        self.crc = crcmod.predefined.mkCrcFun('modbus')
        self.logger = logging.getLogger('modbus')
        self.ser = None
        try:
            s = serial.Serial(port, baudrate, serial.EIGHTBITS,
                              serial.PARITY_NONE, serial.STOPBITS_ONE,
                              timeout=timeout)
        except serial.SerialException:
            raise ModbusError("Cannot open serial on port %s" % port)
        self.ser = s

    def __del__(self):
        if self.ser is not None:
            self.ser.close()
            self.ser = None

    def send(self, data, n):
        """Append CRC to data and send through serial.
Receive response and check CRC.
data    - data to send (without CRC)
n       - expected size of response (without CRC)
return data with CRC stripped
"""
        data += pack('<H', self.crc(data))
        self.logger.debug(" => %s %s [%s]",
                          data[:2].hex(), data[2:-2].hex(), data[-2:].hex())
        nw = self.ser.write(data)
        if nw < len(data):
            self.logger.debug("written %d" % nw)
            raise ModbusError("Incomplete serial data write")
        if self.echo:
            resp = self.ser.read(nw)
            assert resp == data, 'incorrect echo => %s <= %s' % (
                data.hex(), resp.hex())
        # early detection of Modbus error
        resp = self.ser.read(5)
        if len(resp) < 5:
            self.logger.error("Incomplete serial read: <%s>" % resp.hex())
            raise ModbusError("Incomplete serial data read")
        if resp[1] & 0x80 and len(resp) == 5:
            self.logger.debug("<=  %02X %02X %02X [%04X]" %
                              unpack(">BBBH", resp))
            assert self.crc(resp) == 0, "Wrong CRC code"
            if resp[0] != data[0] or resp[1] & 0x7F != data[1]:
                raise ModbusError("Malformed error response")
            raise ModbusError("Modbus error code", resp[2])
        if n > 3:   # read the rest of data if no error occurred
            resp += self.ser.read(n-3)
            if len(resp) < n+2:
                self.logger.error("Incomplete serial read: <%s>" % resp.hex())
                raise ModbusError("Incomplete serial data read")
        self.logger.debug(
            "<=  %s %s [%s]",
            resp[:2].hex(), resp[2:-2].hex(), resp[-2:].hex())
        if self.ser.in_waiting > 0:
            self.ser.read(self.ser.in_waiting)  # empty read buffer
            raise ModbusError("Surplus data in serial read")
        if resp[1] & 0x80 and len(resp) == 5:
            assert self.crc(resp) == 0, "Wrong CRC code"
            if resp[0] != data[0] or resp[1] & 0x7F != data[1]:
                raise ModbusError("Malformed error response")
            raise ModbusError("Modbus error code", resp[2])
        if len(resp) < n+2 or self.ser.in_waiting > 0:
            raise ModbusError("Incomplete serial data read")
        assert self.crc(resp) == 0, "Wrong CRC code"
        return resp[:-2]

    def read_holding_registers(self, reg_addr, reg_nb=1):
        """Modbus function READ_HOLDING_REGISTERS (0x03)
reg_addr     - register address (0 to 0xFFFF)
reg_nb       - number of registers (1 to 125)
"""
        return self._read_registers(READ_HOLDING_REGISTERS, reg_addr, reg_nb)

    def read_input_registers(self, reg_addr, reg_nb=1):
        """Modbus function READ_INPUT_REGISTERS (0x04)
reg_addr     - register address (0 to 0xFFFF)
reg_nb       - number of registers (1 to 125)
"""
        return self._read_registers(READ_INPUT_REGISTERS, reg_addr, reg_nb)

    def _read_registers(self, ins, reg_addr, reg_nb=1):
        """Modbus function to read registers
ins          - instruction: one of READ_HOLDING_REGISTERS or
                                   READ_INPUT_REGISTERS
reg_addr     - register address (0 to 0xFFFF)
reg_nb       - number of registers (1 to 125)
"""
        assert ins in (READ_HOLDING_REGISTERS, READ_INPUT_REGISTERS), \
            "wrong instruction"
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
        assert hasattr(reg_values, '__iter__'), "reg_valeus not a list"
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

    def write_int_BE(self, reg_addr, ival):
        """Convert int to 2 words (big end) and write them to <reg_addr>"""
        self.write_multiple_registers(reg_addr,
                                      [ival // 0x10000, ival % 0x10000])

    def write_int_LE(self, reg_addr, ival):
        """Convert int to 2 words (little end) and write them to <reg_addr>"""
        self.write_multiple_registers(reg_addr,
                                      [ival % 0x10000, ival // 0x10000])

    def read_int_LE(self, reg_addr):
        """Read 2 words from <reg_addr> and interpret them as LE int"""
        words = self.read_holding_registers(reg_addr, 2)
        return words[0] + (words[1] << 16)

    def read_int_BE(self, reg_addr):
        """Read 2 words from <reg_addr> and interpret them as BE int"""
        words = self.read_holding_registers(reg_addr, 2)
        return (words[0] << 16) + words[1]


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
