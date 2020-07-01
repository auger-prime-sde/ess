"""
   Control of Binder climate chambers

   Petr Tobiska <tobiska@fzu.cz>
"""

import logging
from datetime import datetime, timedelta
from time import sleep
from struct import unpack

from modbus import Modbus, ModbusError, floats2words, words2floats

VERSION = "20200630"


class Binder(object):
    """Abstraction of Binder chamber"""
    mytype = None
    stop_manual = True  # manual state by default after stop

    def load_prog(self, progno, chamberprog):
        """Load ChamberProg into the chamber"""
        raise RuntimeError('Not implemented in base class')

    def start_prog(self, progno):
        """Start program in the chamber"""
        raise RuntimeError('Not implemented in base class')

    def stop_prog(self, manual=False):
        """Stop running program
manual - if True switch to manual mode"""
        raise RuntimeError('Not implemented in base class')

    def get_state(self):
        """Get chamber state
returns: progno of running program or "manual" or "idle"."""
        raise RuntimeError('Not implemented in base class')

    def state_manual(self):
        """Set manual state of the chamber"""
        raise RuntimeError('Not implemented in base class')

    def state_idle(self):
        """Set state of the chamber"""
        raise RuntimeError('Not implemented in base class')

    def get_temp(self):
        """Read actual temperature in the chamber"""
        raise RuntimeError('Not implemented in base class')

    def get_humid(self):
        """Read actual humidity in the chamber"""
        raise RuntimeError('Not implemented in base class')

    def set_temp(self, manual):
        """Set temperature in manual mode
manual - if True, switch to manual mode"""
        raise RuntimeError('Not implemented in base class')

    def set_humid(self, manual):
        """Set humidity in manual mode
manual - if True, switch to manual mode"""
        raise RuntimeError('Not implemented in base class')

    def __del__(self):
        pass


class Binder_MKFT115_MB1(Binder):
    """Interface to Binder MKFT 115 with MB1 controller"""
    mytype = 'MB1'
    # register addresses, MKFT 115 E.2, MB1 controller
    ADDR_ACT_TEMP      = 0x11A9
    ADDR_ACT_HUMID     = 0x11CD
    ADDR_SET_TEMP      = 0x1077
    ADDR_SET_HUMID     = 0x1079
    ADDR_SET_TEMP_MAN  = 0x156F
    ADDR_SET_HUMID_MAN = 0x1571
    ADDR_SET_TEMP_BAS  = 0x1581
    ADDR_SET_HUMID_BAS = 0x1583
    ADDR_PROG_RESET    = 0x1A00
    ADDR_PROG_STATUS   = 0x1A01
    ADDR_PROG_NO       = 0x1A02            # program number
    ADDR_PROG_TYPE     = 0x1A03            # 0 .. temperature, 1 .. humidity
    ADDR_PROG_SEG      = 0x1A04            # segment programmed
    ADDR_PROG_NSEG     = 0x1A05            # total number of segments
    ADDR_PROG_6        = 0x1A06            # ???
    ADDR_PROG_VAL      = 0x1A07            # target value
    ADDR_PROG_GRAD     = 0x1A09            # gradient ?
    ADDR_PROG_LIMI     = 0x1A0B            # limit min
    ADDR_PROG_LIMA     = 0x1A0D            # limit max
    ADDR_PROG_DUR      = 0x1A0F            # duration
    ADDR_PROG_OPERC    = 0x1A11            # operational contacts
    ADDR_PROG_NUM_JUMP = 0x1A12            # number of jumps back
    ADDR_PROG_SEG_JUMP = 0x1A13            # segment to jump back (from 0)
    ADDR_PROG_END      = 0x1599            # end of program

    ADDR_MODE          = 0x1A22
    ADDR_PROGNO        = 0x1A23
    # constants
    STATE_BASIC  = 0x1000
    STATE_MANUAL = 0x0800
    STATE_PROG   = 0x0400
    OP_ANTICOND = 1
    P_TEMP = 0                             # segment type temperature
    P_HUMID = 1                            # segment type humidity
    NPROG = 25                      # max. number of programs (numbered from 0)

    class Segment:
        """Program segment"""
        def __init__(self, val, duration, **kw):
            self.val = val
            self.duration = duration
            self.grad = kw.get('grad', 200004.)
            self.minlim = kw.get('minlim', -999.)
            self.maxlim = kw.get('maxlim', 999.)
            self.operc = kw.get('operc', 0)
            self.r6 = kw.get('r6', 0)
            self.numjump = kw.get('numjump', 0)
            self.segjump = kw.get('segjump', 0)

    def __init__(self, modbus):
        assert isinstance(modbus, Modbus), "Modbus instance expected"
        self.modbus = modbus

    def _reset(self):
        """Some initialization"""
        self.modbus.write_single_register(self.ADDR_PROG_RESET, 5)
        self.modbus.read_holding_registers(self.ADDR_PROG_RESET)
        self.modbus.read_holding_registers(self.ADDR_PROG_STATUS)

    def start_prog(self, progno):
        assert 0 <= progno < self.NPROG, "Incorrect Prog No"
        self.get_state()  # read current state
        self.modbus.write_single_register(self.ADDR_MODE, 0)
        self.modbus.write_single_register(self.ADDR_PROGNO, progno)
        self.modbus.write_single_register(self.ADDR_MODE, self.STATE_PROG)

    def stop_prog(self, manual=None):
        if manual is None:
            manual = self.stop_manual
        mode = self.STATE_MANUAL if manual else self.STATE_BASIC
        self.modbus.write_single_register(self.ADDR_MODE, mode)

    def get_state(self):
        """Read state from chamber"""
        resp = self.modbus.read_holding_registers(self.ADDR_MODE)[0]
        if resp & self.STATE_PROG:
            return 'prog'
        elif resp & self.STATE_MANUAL:
            return 'manual'
        return 'idle'

    def state_manual(self):
        self.modbus.write_single_register(self.ADDR_MODE, self.STATE_MANUAL)

    def state_idle(self):
        self.modbus.write_single_register(self.ADDR_MODE, self.STATE_BASIC)

    def get_temp(self):
        return self.modbus.read_float(self.ADDR_ACT_TEMP)

    def get_humid(self):
        return self.modbus.read_float(self.ADDR_ACT_HUMID)

    def set_temp(self, temperature, manual=True):
        self.modbus.write_float(self.ADDR_SET_TEMP, temperature)
        if manual:
            self.modbus.write_single_register(self.ADDR_MODE, self.MODE_MANUAL)

    def set_humid(self, humidity, manual=True):
        self.modbus.write_float(self.ADDR_SET_HUMID, humidity)
        if manual:
            self.modbus.write_single_register(self.ADDR_MODE, self.MODE_MANUAL)

    def convert_chamber2binder(self, chamberprog):
        """Convert ChamberProg into temp/humid segments
returns (seg_temp, seg_humid)
        seg_temp, seg_humid - list of BinderMB1Segment, humid may be None"""
        seg_temp = []
        seg_humid = []
        temp_prev = chamberprog.temperature
        humid_prev = chamberprog.humidity
        anticond_prev = chamberprog.anticond
        cycle_start = None
        cycle_ind = 0 if len(chamberprog.cycles) > 0 else None
        for i, seg in enumerate(chamberprog.segments):
            dur = seg['duration']
            temp_end = seg.get('temperature', None)
            if temp_end is None:
                temp_end = temp_prev
            anticond = seg.get('anticond', None)
            if anticond is None:
                anticond = anticond_prev
            operc = self.OP_ANTICOND if anticond else 0
            tseg = Binder_MKFT115_MB1.Segment(
                temp_prev, dur, operc=operc)
            # check for cycle start
            if(cycle_ind is not None and
               seg is chamberprog.cycles[cycle_ind][0]):
                assert cycle_start is None, "Nested cycles"
                cycle_start = i
            # check for cycle end
            if(cycle_ind is not None and
               seg is chamberprog.cycles[cycle_ind][2]):
                assert cycle_start is not None, "Cycle end while not in cycle"
                numrepeat = chamberprog.cycles[cycle_ind][1]
                if numrepeat > 0:
                    tseg.numjump = numrepeat - 1
                    tseg.segjump = cycle_start
                else:  # discard zero-repeat cycles
                    seg_temp = seg_temp[:cycle_start]
                    if humid_prev is not None:
                        seg_humid = seg_humid[:cycle_start]
                cycle_start = None
                cycle_ind += 1
                if len(chamberprog.cycles) == cycle_ind:
                    cycle_ind = None
                if numrepeat == 0:
                    continue
            seg_temp.append(tseg)
            temp_prev = temp_end
            anticond_prev = anticond
            if humid_prev is not None:
                humid_end = seg.get('humidity', None)
                if humid_end is None:
                    humid_end = humid_prev
                hseg = Binder_MKFT115_MB1.Segment(
                    humid_prev, dur,
                    numjump=tseg.numjump, segjump=tseg.segjump)
                seg_humid.append(hseg)
                humid_prev = humid_end
        # the last segment
        assert cycle_start is None, "Unfinished cycle"
        seg_temp.append(Binder_MKFT115_MB1.Segment(temp_prev, 1))
        if humid_prev is not None:
            seg_humid.append(Binder_MKFT115_MB1.Segment(humid_prev, 1))
        else:
            seg_humid = None
        return (seg_temp, seg_humid)

    def load_prog(self, progno, chamberprog):
        """Load ChamberProg inside the chamber"""
        assert 0 <= progno < self.NPROG, "Incorrect Prog No"
        nseg = len(chamberprog.segments)
        assert nseg < 100, "Max. 100 segments allowed"
        m = self.modbus
        # clear existing program TBD
        self._reset()
        seg_temp, seg_humid = self.convert_chamber2binder(chamberprog)
        segments = [(self.P_TEMP, seg_temp)]
        if seg_humid is not None:
            segments.append((self.P_HUMID, seg_humid))

        for segtype, segs in segments:
            m.write_single_register(self.ADDR_PROG_NO, progno)
            m.write_single_register(self.ADDR_PROG_TYPE, segtype)
            m.write_single_register(self.ADDR_PROG_NSEG, nseg+1)

            for i, s in enumerate(iter(segs)):
                m.write_single_register(self.ADDR_PROG_SEG, i)
                m.write_single_register(self.ADDR_PROG_6, s.r6)
                m.write_float(self.ADDR_PROG_VAL, s.val)
                m.write_float(self.ADDR_PROG_GRAD, s.grad)
                m.write_int_BE(self.ADDR_PROG_DUR, s.duration)
                m.write_single_register(self.ADDR_PROG_NUM_JUMP, s.numjump)
                m.write_single_register(self.ADDR_PROG_SEG_JUMP, s.segjump)
                m.write_float(self.ADDR_PROG_LIMI, s.minlim)
                m.write_float(self.ADDR_PROG_LIMA, s.maxlim)
                if segtype == self.P_TEMP:
                    m.write_single_register(self.ADDR_PROG_OPERC, s.operc)
                self._reset()
        m.write_single_register(self.ADDR_PROG_END, 0)


class Binder_MKFT115_MB2(Binder):
    """Interface to Binder MKFT 115 with MB2 controller"""
    mytype = 'MB2'
    ADDR_PROCVAL_TEMP   = 0x1004  # deg.C
    ADDR_PROCVAL_DOOR   = 0x1006  # deg.C
    ADDR_PROCVAL_OIL    = 0x1008  # deg.C
    ADDR_PROCVAL_HUMID  = 0x100a  # % R.H.
    ADDR_PROCVAL_COMPH  = 0x100c  # deg.C

    ADDR_CURR_PROGNO    = 0x10a4
    ADDR_CURR_PROGSEG   = 0x10a5
    ADDR_PROGTIME_ELAPS = 0x10a6  # second, MSW, LSW
    ADDR_PROGTIME_REM   = 0x10a8  # second, MSW, LSW

    ADDR_SET_TEMP       = 0x10b2  # RO float
    ADDR_SET_HUMI       = 0x10b4  # RO float
    ADDR_SET_FAN        = 0x10b6  # RO float
    ADDR_CURR_TEMP      = 0x10d2  # float, like 0x10d0 & _PROCVAL_
    ADDR_CURR_HUMID     = 0x10d6  # float, like 0x10d4 & _PROCVAL_

    ADDR_DATETIME_PROG  = 0x1140  # Y, m, d, H, M, S
    ADDR_PROGRUN_INIT   = 0x1146
    ADDR_PROGRUN_NO     = 0x1147  # from 0
    ADDR_PROGRUN_SEG    = 0x1148  # from 1
    ADDR_PROGRUN_START  = 0x1149  # write 1 to start the program
    ADDR_PROGRUN_STOP   = 0x114a  # write 1 to stop the program
    ADDR_PROGRUN_PAUSE  = 0x114b  # write 1 to pause the program
    ADDR_SET_TEMP_MAN   = 0x114c  # float
    ADDR_SET_HUMID_MAN  = 0x114e  # float
    ADDR_SET_FAN_MAN    = 0x1150  # float
    ADDR_OPERC_MAN      = 0x1158
    ADDR_OPERC          = 0x1292
    ADDR_DATETIME_CURR  = 0x134a  # Y, m, d, H, M, S

    # program manipulation
    ADDR_PGM_CTRL       = 0x3000  # control/status, LSW, MSW = 0000
    ADDR_PGM_STATUS     = 0x3002
    ADDR_PGM_NSEG       = 0x3003
    ADDR_PGM_PROGNO     = 0x3004  # from 0
    ADDR_PGM_FLOATS     = 0x3014  # 18*2W, + <iseg>*SEG_LENGTH
    ADDR_PGM_DURATION   = 0x3038  # seconds, LSW, MSW
    ADDR_PGM_OPERCONT   = 0x303c  # <MSB,LSB>
    ADDR_PGM_REPEAT     = 0x303d  # <where><count>, <where> from 1,
    #   <count> number of jumps (passes = #jumps + 1)
    ADDR_PGM_SEGTYPE    = 0x3042  # 2W, SG_RAMP/SG_STEP
    ADDR_PGM_TITLE      = 0x4300  # 32W, plain ascii, zero padded
    LEN_PGM_TITLE = 32  # length of PGM_TITLE in words
    LEN_SEGMENT = 0x30  # length of segment
    LEN_FLOATS = 2*18   # number of W in _PGM_FLOATS

    MASK_PGM_BUSY   = 0x0080  # PGM_CTRL.LSW busy
    CMD_PGM_DELETE  = 0x0006  # delete the program
    CMD_PGM_LOAD    = 0x000b  # load the program from NVM
    CMD_PGM_STORE   = 0x000c  # store the program to NVM
    CMDS = (CMD_PGM_DELETE, CMD_PGM_LOAD, CMD_PGM_STORE)

    SG_RAMP = 0x00000000  # segment type, 2 words LE
    SG_STEP = 0x00000001

    # 1292 RO / 1158 RW (in manual mode)
    OP_HUMIDOFF = 1 << 0
    OP_IDLEMODE = 1 << 1
    OP_SWITCH1  = 1 << 2
    OP_SWITCH2  = 1 << 3
    OP_SWITCH3  = 1 << 4
    OP_SWITCH4  = 1 << 5
    OP_ANTICOND = 1 << 6
    OP_CAIRVALVE = 1 << 7
    OP_CAIRDRY  = 1 << 8

    NPROG = 25  # max. number of programs (numbered from 0)
    NSEG = 100  # max. number of segments in program
    TOUT = 0.01  # timeout in _pgm_ctrl, seconds

    class Bprog(object):
        """BinderMB2 relevant info about program"""
        def __init__(self, champrog):
            self.title = champrog.title
            self.last_temp = champrog.temperature
            self.last_anticond = champrog.anticond
            if champrog.humidity is None:
                self.zHumid = False
                self.last_humid = 60.0  # value in program if not provided
            else:
                self.zHumid = True
                self.last_humid = champrog.humidity
            self.limit_temp_low = -260.0
            self.limit_temp_high = 260.0
            self.limit_humid_low = -98.0
            self.limit_humid_high = 98.0

    class Segment(object):
        """Program segment"""
        def __init__(self, duration, bprog, **kw):
            """Constructor
bprog - BinderMB2 relevant info about program
kw - segment parametes:
  duration - mandatory, all other optional (or None)
  temperature (end), humidity (end), anticond, segnum, segjump
"""
            self.duration = duration
            # temp, humid, 4 x 0.0, temp_low, humid_low, 4 x 0.0,
            # temp_high, humid_high, 4 x 0.0
            self.flblob = (
                floats2words(bprog.last_temp, bprog.last_humid) +
                [0] * 8 +
                floats2words(bprog.limit_temp_low, bprog.limit_humid_low) +
                [0] * 8 +
                floats2words(bprog.limit_temp_high, bprog.limit_humid_high) +
                [0] * 8)
            temp = kw.get('temperature', None)
            if temp is not None:
                bprog.last_temp = temp
            humid = kw.get('humidity', None)
            if humid is not None:
                bprog.last_temp = humid
            anticond = kw.get('anticond', None)
            if anticond is None:
                anticond = bprog.last_anticond
            else:
                bprog.last_anticond = anticond
            self.operc = Binder_MKFT115_MB2.OP_ANTICOND if anticond else 0
            if not bprog.zHumid:
                self.operc |= Binder_MKFT115_MB2.OP_HUMIDOFF
            segjump = kw.get('segjump', 1)
            numjump = kw.get('numjump', 0)
            self.jumps = (segjump << 8) + numjump

    def __init__(self, modbus):
        assert isinstance(modbus, Modbus), "Modbus instance expected"
        self.modbus = modbus
        self.logger = logging.getLogger('BinderMB2')

    def _pgm_ctrl(self, cmd):
        """Program control, write cmd and wait until busy
return status"""
        assert cmd in self.CMDS, "unknown command %s" % repr(cmd)
        self.modbus.write_multiple_registers(
            self.ADDR_PGM_CTRL, [cmd | self.MASK_PGM_BUSY, 0])
        while True:
            res = self.modbus.read_holding_registers(self.ADDR_PGM_CTRL)[0]
            if not res & self.MASK_PGM_BUSY:
                break
            sleep(self.TOUT)
        return self.modbus.read_holding_registers(self.ADDR_PGM_STATUS)[0]

    def _read_pgm_title(self):
        """Read title of current program from chamber"""
        b = bytearray()
        for w in self.modbus.read_holding_registers(
                self.ADDR_PGM_TITLE, self.LEN_PGM_TITLE):
            b.append(w // 0x100)
            b.append(w % 0x100)
        return bytes(b.rstrip(b'\0'))

    def _write_pgm_title(self, bstr):
        """Write title of current program into chamber"""
        words = unpack('>%dH' % self.LEN_PGM_TITLE,
                       bstr.ljust(2*self.LEN_PGM_TITLE, b'\0'))
        self.modbus.write_multiple_registers(self.ADDR_PGM_TITLE, words)

    def _prog_running(self):
        """Check if a program is runnning"""
        etime = self.modbus.read_int_BE(self.ADDR_PROGTIME_ELAPS)
        rtime = self.modbus.read_int_BE(self.ADDR_PROGTIME_REM)
        if etime == rtime == 0:
            return False
        elif etime > 0 and rtime > 0:
            return True
        self.logger.debug('elapsed time = %d, remaining time = %d ??',
                          etime, rtime)
        return etime > 0

    def _read_time(self, addr):
        """Read 6W from <addr> and interpret them as datetime
return datetime object"""
        words = self.modbus.read_holding_registers(addr, 6)
        return datetime(*words)

    def _write_time(self, addr, dt):
        """Write datetime as 6W to <addr>
dt - instance of datetime"""
        words = (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
        self.modbus.write_multiple_registers(addr, words)

    def _scan_programs(self):
        """Read programs from chamber"""
        progs = {}
        m = self.modbus
        for progno in range(self.NPROG):
            m.write_single_register(self.ADDR_PGM_PROGNO, progno)
            m.write_single_register(self.ADDR_PGM_NSEG, self.NSEG)
            if self._pgm_ctrl(self.CMD_PGM_LOAD) == 0:
                progs[progno] = self._read_pgm_title()
        return progs

    def _read_program(self, progno):
        """Read program from chamber
Return title, list of dicts:
   duration, floats[], operc, numjump, segjump, segtype"""
        m = self.modbus
        m.write_single_register(self.ADDR_PGM_PROGNO, progno)
        m.write_single_register(self.ADDR_PGM_NSEG, self.NSEG)
        if self._pgm_ctrl(self.CMD_PGM_LOAD) != 0:
            return None
        nseg = m.read_holding_registers(self.ADDR_PGM_NSEG)[0]
        title = self._read_pgm_title()
        segments = []
        for i in range(nseg):
            seg = {}
            seg['floats'] = words2floats(*m.read_holding_registers(
                self.ADDR_PGM_FLOATS + i*self.LEN_SEGMENT, self.LEN_FLOATS))
            seg['segtype'] = m.read_int_LE(
                self.ADDR_PGM_SEGTYPE + i*self.LEN_SEGMENT)
            seg['duration'] = m.read_int_LE(
                self.ADDR_PGM_DURATION + i*self.LEN_SEGMENT)
            jumps = m.read_holding_registers(
                self.ADDR_PGM_REPEAT + i*self.LEN_SEGMENT)[0]
            seg['numjump'] = jumps % 0x100
            seg['segjump'] = jumps // 0x100
            seg['operc'] = m.read_holding_registers(
                self.ADDR_PGM_OPERCONT + i*self.LEN_SEGMENT)[0]
            segments.append(seg)
        return title, segments

    def get_state(self):
        """Read state from chamber"""
        if self._prog_running():
            return self.modbus.read_holding_registers(self.ADDR_PROGRUN_NO)[0]
        operc = self.modbus.read_holding_registers(self.ADDR_OPERC)[0]
        return 'idle' if operc & self.OP_IDLEMODE else 'manual'

    def state_manual(self):
        if self._prog_running():
            self.stop_prog(manual=True)
        else:
            operc = self.modbus.read_holding_registers(self.ADDR_OPERC)[0]
            operc &= ~self.OP_IDLEMODE
            self.modbus.write_single_register(self.ADDR_OPERC_MAN, operc)

    def state_idle(self):
        if self._prog_running():
            self.stop_prog(manual=False)
        else:
            operc = self.modbus.read_holding_registers(self.ADDR_OPERC)[0]
            operc |= self.OP_IDLEMODE
            self.modbus.write_single_register(self.ADDR_OPERC_MAN, operc)

    def start_prog(self, progno, seg=1, delay=0):
        """Start program <progno> in the chamber"""
        stime = self._read_time(self.ADDR_DATETIME_CURR)
        tdelta = (stime - datetime.now()).total_seconds()
        self.logger.info('Starting prog %d, time diff %d seconds',
                         progno, tdelta)
        stime += timedelta(seconds=delay)
        self.modbus.write_single_register(self.ADDR_PROGRUN_SEG, seg)
        self.modbus.write_single_register(self.ADDR_PROGRUN_INIT, 0)
        self.modbus.write_single_register(self.ADDR_PROGRUN_NO, progno)
        self._write_time(self.ADDR_DATETIME_PROG, stime)
        self.modbus.write_single_register(self.ADDR_PROGRUN_START, 1)

    def stop_prog(self, manual=None):
        if manual is None:
            manual = self.stop_manual
        self.modbus.write_single_register(self.ADDR_PROGRUN_STOP, 1)
        operc = self.modbus.read_holding_registers(self.ADDR_OPERC)[0]
        if manual and operc & self.OP_IDLEMODE:
            operc &= ~self.OP_IDLEMODE
            self.modbus.write_single_register(self.ADDR_OPERC_MAN, operc)
        elif not manual and not operc & self.OP_IDLEMODE:
            operc |= self.OP_IDLEMODE
            self.modbus.write_single_register(self.ADDR_OPERC_MAN, operc)

    def get_temp(self):
        return self.modbus.read_float(self.ADDR_CURR_TEMP)

    def get_humid(self):
        return self.modbus.read_float(self.ADDR_CURR_HUMID)

    def get_target(self):
        """Read target temperature & humidity (both prog/manual modes)
return temperature, humidity"""
        assert self.ADDR_SET_HUMID == self.ADDR_SET_TEMP + 2
        return words2floats(*self.modbus.read_holding_registers(
            self.ADDR_SET_TEMP, 4))

    def set_temp(self, temperature):
        self.modbus.write_float(self.ADDR_SET_TEMP_MAN, temperature)

    def set_humid(self, humidity):
        self.modbus.write_float(self.ADDR_SET_HUMID_MAN, humidity)

    def load_prog(self, progno, chamberprog):
        """Load ChamberProg inside the chamber"""
        assert 0 <= progno < self.NPROG, "Incorrect Prog No"
        nseg = len(chamberprog.segments)
        assert nseg < self.NSEG, "Max. %d segments allowed" % self.NSEG
        self.logger.info('Loading program %d, %d segments', progno, nseg)
        segments = self.convert_chamber2binder(chamberprog)
        m = self.modbus
        m.write_single_register(self.ADDR_PGM_PROGNO, progno)
        m.write_single_register(self.ADDR_PGM_NSEG, self.NSEG)
        status = self._pgm_ctrl(self.CMD_PGM_DELETE)
        if status != 0:
            self.logger.error('Error deleting prog %d, status %d',
                              progno, status)
            return
        m.write_single_register(self.ADDR_PGM_PROGNO, progno)
        m.write_single_register(self.ADDR_PGM_NSEG, nseg+1)
        self._write_pgm_title(chamberprog.title)
        for i, seg in enumerate(segments):
            m.write_int_LE(
                self.ADDR_PGM_DURATION + i*self.LEN_SEGMENT, seg.duration)
            m.write_int_LE(
                self.ADDR_PGM_SEGTYPE + i*self.LEN_SEGMENT, self.SG_RAMP)
            m.write_single_register(
                self.ADDR_PGM_REPEAT + i*self.LEN_SEGMENT, seg.jumps)
            m.write_multiple_registers(
                self.ADDR_PGM_FLOATS + i*self.LEN_SEGMENT, seg.flblob)
            m.write_single_register(
                self.ADDR_PGM_OPERCONT + i*self.LEN_SEGMENT, seg.operc)
        status = self._pgm_ctrl(self.CMD_PGM_STORE)
        if status != 0:
            self.logger.error('Error storing prog %d, status %d',
                              progno, status)
        else:
            self.logger.info('Program %d stored', progno)

    def convert_chamber2binder(self, chamberprog):
        """Convert ChamberProg into Segments
returns list of BinderMB2Segments"""
        bprog = self.Bprog(chamberprog)
        segments = []
        cycle_start = None
        cycle_ind = 0 if len(chamberprog.cycles) > 0 else None
        for i, seg in enumerate(chamberprog.segments):
            duration = seg['duration']
            kw = {key: seg[key]
                  for key in ('temperature', 'humidity', 'anticond')
                  if key in seg}
            # check for cycle start
            if(cycle_ind is not None and
               seg is chamberprog.cycles[cycle_ind][0]):
                assert cycle_start is None, "Nested cycles"
                cycle_start = i
            # check for cycle end
            if(cycle_ind is not None and
               seg is chamberprog.cycles[cycle_ind][2]):
                assert cycle_start is not None, "Cycle end while not in cycle"
                numrepeat = chamberprog.cycles[cycle_ind][1]
                if numrepeat > 1:
                    kw['numjump'] = numrepeat-1
                    kw['segjump'] = cycle_start+1
                # for numrepeat == 1 do nothing extra
                elif numrepeat == 0:  # discard zero-repeat cycles
                    segments = segments[:cycle_start]
                cycle_start = None
                cycle_ind += 1
                if len(chamberprog.cycles) == cycle_ind:
                    cycle_ind = None
                if numrepeat == 0:
                    continue
            segments.append(self.Segment(duration, bprog, **kw))
        # the last segment
        assert cycle_start is None, "Unfinished cycle"
        segments.append(self.Segment(1, bprog))
        return segments


BinderTypes = {bcls.mytype: bcls for bcls in (
    Binder_MKFT115_MB1,
    Binder_MKFT115_MB2,
    )}


def getBinder(port):
    """Try to open Modbus(port) and determine Binder instrument
returns Binder instance or None in case of failure"""
    logger = logging.getLogger('getBinder')
    try:
        m = Modbus(port)
        logger.info('Opening serial %s', repr(m.ser))
    except (FileNotFoundError, ModbusError):
        logger.exception('Opening serial port failed')
        m.ser.close()
        return None

    b = None
    for bcls in BinderTypes.values():
        try:
            b = bcls(m)
            b.get_state()
        except ModbusError:
            if b is not None:
                b.__del__()
                b = None
            continue
        break
    m.ser.close()
    return b
