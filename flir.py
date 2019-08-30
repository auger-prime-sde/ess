"""
   ESS procedure
   communication with FLIR A40M IR camera
"""

import re
import threading
import logging
from time import sleep
from struct import unpack
from serial import Serial

from BME import readSerRE, SerialReadTimeout


class FLIR(threading.Thread):
    """Thread managing FLIR camera"""
    # baudrates accepted by FLIR
    BAUDRATES = (19200, 115200, 9600, 38400, 57600)
    MAXBAUDRATE = 115200
    BLOCKSIZE = 512
    TOUT = 0.3
    IMTYPES = {'p': '.fff', 'o': '.fff', 'e': '.jpg', 'j': '.jpg'}
    re_cmd = re.compile(br'(.*)\r\nOK>', re.DOTALL)
    re_fblock = re.compile(br'\r\nOK\r\n' +
                           br'(?P<size>..)(?P<chksum>..)(?P<data>.*)\r\nOK>',
                           re.DOTALL)
    re_baudrate = re.compile(br'.*\r\n(?P<baudrate>\d+)\r\nOK>', re.DOTALL)

    def __init__(self, port, timer, q_att, datadir, uubnum=0, imtype=None):
        """Detect baudrate, set termecho to off and switch baudrate to max"""
        super(FLIR, self).__init__()
        self.timer,  self.q_att, self.datadir = timer, q_att, datadir
        self.uubnum = uubnum
        self.logger = logging.getLogger('FLIR')
        self.ser = s = None   # avoid NameError on isinstance(s, Serial) check
        try:
            s = self._detect_baudrate(port)
            s.write(b'termecho off\n')
            readSerRE(s, FLIR.re_cmd, timeout=FLIR.TOUT)
            if s.baudrate < FLIR.MAXBAUDRATE:
                self._set_baudrate(FLIR.MAXBAUDRATE, s)
        except SerialReadTimeout:
            self.logger.exception("Init serial with FLIR failed")
            if isinstance(s, Serial) and s.isOpen():
                self.logger.info('Closing serial %s', s.port)
                s.close()
            raise
        self.ser = s
        resp = self._send_recv('version')
        self.logger.info('detected: %s', resp.splitlines()[1])
        # <store> parameter and filename suffix
        if imtype not in FLIR.IMTYPES.keys():
            if imtype is not None:
                self.logger.error('Unknown image type "%s" ignored', imtype)
            imtype = 'p'  # default: .fff with PNG rawdata
        self.typ = (imtype, FLIR.IMTYPES[imtype])

    def __del__(self):
        if isinstance(self.ser, Serial) and self.ser.isOpen():
            self.ser.close()

    def _detect_baudrate(self, port):
        """Try all possible baudrates
return open Serial connection or raise exception"""
        s = None
        for brate in FLIR.BAUDRATES:
            try:
                self.logger.debug('trying baudrate %d', brate)
                s = Serial(port, baudrate=brate)
                s.write(b'\n')
                readSerRE(s, FLIR.re_cmd, timeout=FLIR.TOUT)
                self.logger.debug('baudrate %d detected', brate)
                return s
            except Exception:
                if isinstance(s, Serial) and s.isOpen():
                    s.close()
                s = None
        raise SerialReadTimeout

    def _set_baudrate(self, baudrate, ser=None):
        """Set a new baudrate on FLIR and on self.ser
baudrate - required baudrate
ser - serial instance, if None, use self.ser
      (may be called from __init__ when self.ser is not defined yet)"""
        assert baudrate in FLIR.BAUDRATES
        if ser is None:
            ser = self.ser
        self.logger.info('Setting baudrate %d', baudrate)
        ser.write(b'baudrate %d\n' % baudrate)
        ser.read_until(b'\r\n')
        ser.baudrate = baudrate
        sleep(0.1)
        ser.write(b'baudrate\n')
        resp = readSerRE(ser, FLIR.re_baudrate,
                         timeout=FLIR.TOUT, logger=self.logger)
        newbaudrate = int(FLIR.re_baudrate.match(resp).groupdict()['baudrate'])
        assert baudrate == newbaudrate
        self.logger.info('Baudrate %d set', baudrate)

    def _send_recv(self, cmd, timeout=1):
        """Send a command and receive a response
cmd - str with command
return the response (before 'OK>', as str)"""
        self.ser.write(bytes(cmd, 'ascii') + b'\n')
        resp = readSerRE(self.ser, FLIR.re_cmd,
                         timeout=timeout, logger=self.logger)
        m = FLIR.re_cmd.match(resp)
        return m.groups()[0].decode('ascii')

    def _getfblock(self, path, seek, size):
        """get data from file
path - path to file
seek - offset
size - size of block to get
return the data block"""
        cmd = 'getfblock "%s" %d %d' % (path, seek, size)
        self.ser.write(bytes(cmd, 'ascii') + b'\n')
        resp = readSerRE(self.ser, FLIR.re_fblock, timeout=2, logger=None)
        d = FLIR.re_fblock.match(resp).groupdict()
        rsize = unpack('>H', d['size'])[0]
        rchksum = unpack('>H', d['chksum'])[0]
        chksum = sum(d['data']) % 0x10000
        assert rsize == len(d['data'])
        assert chksum == rchksum
        return d['data']

    def getfile(self, flirpath, fname):
        """Get file <flirpath> from FLIR and store as <fname>"""
        self.logger.info('Storing %s to %s', flirpath, fname)
        offset = 0
        with open(fname, 'wb') as fp:
            while True:
                block = self._getfblock(flirpath, offset, FLIR.BLOCKSIZE)
                if len(block) == 0:
                    break
                fp.write(block)
                offset += len(block)
        self.logger.info('done')

    def snapshot(self, imagename):
        """Take a snapshot on FLIR"""
        flirpath = 'images/' + imagename
        self.logger.info('Snapshot to %s', flirpath)
        cmd = 'store -%s %s' % (self.typ[0], flirpath)
        self._send_recv(cmd, timeout=15)

    def listimages(self):
        """Return list of all images on FLIR"""
        return self._send_recv('ls -1 images').splitlines()[1:]

    def deleteimage(self, imagename):
        """Delete <image> in dir images/"""
        self._send_recv('rm images/%s%s' % (imagename, self.typ[1]))

    def run(self):
        snapshots = {}   # images stored during the session
        downloaded = []
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                self.logger.info('Timer stopped, ending run()')
                return
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags.get('flir', None)
            # flags = { imagename: <str>,
            #           attname: <str> - optional, use imagename if not present
            #           description: <str> - optional
            #           snapshot: True/False - imagename mandatory
            #           download: True/False - download <imagename>
            #                                  or all images if not present
            #           delete:   True/False - delete <imagename>
            #                                  or all images if not present
            if flags is None:
                continue
            imagename = flags.get('imagename', None)
            if flags.get('snapshot', False):
                if imagename is None:
                    self.logger.error('imagename missing in snapshot')
                else:
                    self.snapshot(imagename)
                    self.logger.info('Image %s stored', imagename)
                    attname = flags.get('attname', imagename)
                    rec = {'name': attname,
                           'uubs': (self.uubnum, ),
                           'timestamp': timestamp}
                    if 'description' in flags:
                        rec['description'] = flags['description']
                    snapshots[imagename] = rec
            if flags.get('download', False):
                if imagename is not None:
                    if imagename in snapshots:
                        imagelist = (imagename, )
                    else:
                        self.logger.error('image <%s> not stored', imagename)
                        imagelist = []
                else:
                    imagelist = snapshots.keys()
                for image in imagelist:
                    fname = image + self.typ[1]
                    self.getfile('images/' + fname, self.datadir + fname)
                    rec = snapshots.pop(image)
                    rec['filename'] = fname
                    self.q_att.put(rec)
                    downloaded.append(image)
            if flags.get('delete', False):
                if imagename is not None:
                    if imagename in snapshots:
                        self.logger.warning(
                            'image %s to be deleted not downloaded', imagename)
                        imagelist = (imagename, )
                    elif imagename not in downloaded:
                        self.logger.error('image <%s> not stored', imagename)
                        imagelist = []
                    else:
                        imagelist = (imagename, )
                else:
                    imagelist, downloaded = downloaded, []
                for image in imagelist:
                    self.logger.info('Deleting image %s', image)
                    self.deleteimage(image)
