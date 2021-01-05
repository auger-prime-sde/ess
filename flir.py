"""
   ESS procedure
   communication with FLIR A40M IR camera
"""

import os
import re
import subprocess
import io
import json
import logging
import threading
from time import sleep
from struct import unpack
from serial import Serial
import PIL.Image
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib

from BME import readSerRE, SerialReadTimeout
from dataproc import item2label
from threadid import syscall, SYS_gettid

EXIFTOOL = "/usr/bin/exiftool"
MAXX = 320
MAXY = 240
try:
    stat = os.stat(EXIFTOOL)
    HAS_EXIFTOOL = True
except FileNotFoundError:
    HAS_EXIFTOOL = False


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
    eval_title_template = 'Temperature increase [deg.C]' + \
        ' on UUB #{uubnum:04d} ({timestamp:%Y-%m-%d}): {res}'
    eval_imname = 'flirtemp_{uubnum:04d}.png'
    eval_draw_type = 'temp'
    eval_draw_clim = (0.0, 15.0)
    COLDHOT = {'irad': (1, 2), 'arad': (3, 4), 'temp': (5, 6)}
    FLIR_RES = {True: 'PASSED', False: 'FAILED', None: 'system error'}

    def __init__(self, port, timer, q_resp, q_att, datadir, uubnum=0,
                 imtype=None, flireval=None, elogger=None):
        """Detect baudrate, set termecho to off and switch baudrate to max"""
        super(FLIR, self).__init__(name='Thread-FLIR')
        self.timer, self.q_resp, self.q_att = timer, q_resp, q_att
        self.datadir, self.elogger = datadir, elogger
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
        self.fe = None
        if flireval is not None:
            self.fe = FlirEval(datadir, timer.basetime, uubnum, **flireval)

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
        if rsize != len(d['data']) or chksum != rchksum:
            self.logger.error('%s => %s', cmd, repr(resp))
            raise AssertionError
        return d['data']

    def _evalFFF(self, fname, timestamp, imname=None, tittemplate=None):
        """Evaluate stored FLIR image
fname - fullname of FLIR image
imname - optional image name (relative to datadir)
title_template - optional image title
return True/False or None in case of error + full image name"""
        if self.fe.bgimage is None:
            self.logger.error('FLIR background not stored, eval not possible')
            return None, None
        if tittemplate is None:
            tittemplate = self.eval_title_template
        if imname is None:
            imname = self.eval_imname
        cold, hot = FLIR.COLDHOT[self.eval_draw_type]
        try:
            self.eval_draw_clim
            rad = self.fe.readFFF(fname)
            res = self.fe.evalFFF(rad)
            restxt = 'PASSED' if res[0] else 'FAILED'
            comps = {label: 'white'
                     for label in self.fe.board.components.keys()}
            for label in res[cold]:
                comps[label] = 'blue'
            for label in res[hot]:
                comps[label] = 'red'
            d = {'uubnum': self.uubnum,
                 'timestamp': timestamp,
                 'res': restxt}
            imfname = self.datadir + imname.format(**d)
            title = tittemplate.format(**d)
            if self.eval_draw_type == 'temp':
                arr = self.fe.rad2temperature(rad) - \
                    self.fe.rad2temperature(self.fe.bgimage)
            else:
                arr = rad - self.fe.bgimage
                self.fe.board.saveImage(
                    arr, imfname, title=title, clim=self.eval_draw_clim,
                    drawComps=comps)
        except Exception:
            msg = 'FLIR eval raised exception'
            if self.elogger is not None:
                enum = self.elogger.log(
                    fname=fname, imname=imname, tittemplate=tittemplate,
                    res=res)
                msg += ' %04d' % enum
            self.logger.error(msg)
            return None, None
        return res[0], imfname

    def _checkFlags(self, flags):
        """Check consistency of flags.
return flags, rec or None, None in case of inconsistency"""
        if flags is None:
            return None, None
        cflags = {action: bool(flags.get(action, False))
                  for action in ('snapshot', 'download', 'delete')}
        if not cflags:
            self.logger.error('No action in flags')
            return None, None
        imagename = flags.get('imagename', None)
        if cflags['snapshot'] and imagename is None:
            self.logger.error('imagename missing in snapshot')
            return None, None
        if cflags['snapshot'] and imagename in self.snapshots:
            self.logger.error('duplicate imagename %s', imagename)
            return None, None
        cflags['imagename'] = imagename
        if cflags['snapshot']:
            rec = {}
            db = flags['db']
            if db is None:
                rec['db'] = None
            else:
                if db not in ('raw', 'eval', 'both'):
                    self.logger.error('unknown db paramter %s', db)
                    return None, None
                rec['db'] = db
            if db == 'raw':
                rec['rawname'] = flags.get('rawname', imagename)
                rec['evalname'] = None
            elif db == 'eval':
                rec['evalname'] = flags.get('evalname', imagename)
            elif db == 'both':
                if any([fn not in flags for fn in ('rawname', 'evalname')]):
                    self.logger.error('missing rawname or evalname')
                    return None, None
                rec['rawname'] = flags['rawname']
                rec['evalname'] = flags['evalname']
            else:
                rec['evalname'] = flags.get('evalname', None)
            bgimage = flags.get('bgimage', False)
            if bgimage and rec['evalname']:
                self.logger.error('Both bgimage and evaluation required')
                return None, None
            rec['bgimage'] = bgimage
            for key in ('description', 'evaltitle', 'evalimname'):
                if key in flags:
                    rec[key] = flags[key]
        else:
            rec = None
        return cflags, rec

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
        tid = syscall(SYS_gettid)
        self.logger.debug('run start, name %s, tid %d',
                          threading.current_thread().name, tid)
        snapshots = {}   # images stored during the session
        downloaded = []
        while True:
            self.timer.evt.wait()
            if self.timer.stop.is_set():
                self.logger.info('Timer stopped, ending run()')
                return
            timestamp = self.timer.timestamp   # store info from timer
            flags = self.timer.flags.get('flir', None)
            # flags = { <one or more actions:>
            #           snapshot: True/False
            #           download: True/False
            #           delete: True/False
            #           <parameters relevant to actions:>
            #           * snapshot
            #             imagename: <str>, mandatory
            #                - internal name, unique during FLIR instance life
            #             db: raw|eval|both
            #                - store raw/eval image into DB, default None
            #             rawname: <str>, conditional if db = raw or both
            #                - attachment name for raw image
            #             evalname: <str>, mandatory if db = eval or both
            #                - attachment name for evaluation image
            #                - implies evaluation
            #             evaltitle: <str>, optional
            #                - eval. image title template for format
            #                - keys: uubnum, res, timestamp
            #             evalimname: <str>, optional
            #                - eval. image name
            #             bgimage: True/False
            #                - use as background for evaluation
            #                - contradicts evalname
            #             description: <str>, optional
            #                - description for raw/eval image to DB
            #           * download, delete
            #             imagename: <str>, optional
            #                - if present limit operation only to imagename
            flags, rec = self._checkFlags(flags)
            if flags is None:
                continue
            imagename = flags['imagename']
            if flags['snapshot']:
                self.snapshot(imagename)
                self.logger.info('Image %s stored', imagename)
                rec['timestamp'] = timestamp
                snapshots[imagename] = rec
            if flags['download']:
                if imagename is not None:
                    if imagename in snapshots:
                        imagelist = (imagename, )
                    else:
                        self.logger.error('image <%s> not stored', imagename)
                        imagelist = []
                else:
                    imagelist = list(snapshots.keys())
                for image in imagelist:
                    fname = image + self.typ[1]
                    self.getfile('images/' + fname, self.datadir + fname)
                    downloaded.append(image)
                    rec = snapshots.pop(image)
                    arec = {'uubs': (self.uubnum, ),
                            'run': True}
                    if 'description' in flags:
                        arec['description'] = rec['description']
                    if rec['db'] in ('raw', 'both'):
                        arec['name'] = rec['rawname']
                        arec['filename'] = self.datadir + fname,
                        self.q_att.put(arec)
                    if rec['bgimage']:
                        self.fe.readFFF(self.datadir + fname, True)
                    elif 'evalname' in flags:
                        res, efname = self._evalFFF(
                            self.datadir + fname, timestamp,
                            rec.get('evalimname', None),
                            rec.get('evaltitle', None))
                        self.timer.add_immediate(
                            'message',
                            'FLIR evaluation: ' + FLIR.FLIR_RES[res])
                        if efname is not None:
                            im = PIL.Image.open(efname)
                            im.show()
                        label = item2label(typ='flireval',
                                           uubnum=self.uubnum)
                        self.q_resp.put({
                            'timestamp': rec['timestamp'],
                            'meas_flir': True,
                            label: res})
                        if rec['db'] in ('eval', 'both') and efname:
                            arec['name'] = rec['evalname']
                            arec['filename'] = efname
                            self.q_att.put(arec)
            if flags['delete']:
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


class FlirEval:
    """Evaluation of FLIR images"""
    METAKEYS = ('PlanckB', 'PlanckF', 'PlanckO', 'PlanckR1', 'PlanckR2')
    CONFIGKEYS = ('fn_points', 'fn_componentsmm', 'fn_pixpoints',
                  'fn_complimits', 'score', 'width')
    MARGINS = {'IRAD': 1, 'ARAD': -1, 'TEMP': -1}  # margins for sum/average

    def __init__(self, datadir, basetime, uubnum, **kwargs):
        assert HAS_EXIFTOOL, 'Exiftool tool not available'
        assert all([key in kwargs for key in FlirEval.CONFIGKEYS])
        board = Board()
        board.readCoordPoints(kwargs['fn_points'])
        board.readComponents(kwargs['fn_componentsmm'])
        pixpoints = Board.readPoints(kwargs['fn_pixpoints'])
        board.calibrate(pixpoints)
        self.board = board
        # {label: [irad_mean, irad_std, +dtto for arad, temp]
        with open(kwargs['fn_complimits']) as fp:
            self.complimits = json.load(fp)
        # irad_minus, irad_plus, arad_minus, arad_plus, temp_minus, temp_plus
        self.score = tuple(kwargs['score'])
        assert len(self.score) == 6
        assert all([isinstance(v, (type(None), int, float))
                    for v in self.score])
        # acceptable width of irad/arad/temp distribuition in component.std
        wid = kwargs['width']
        if isinstance(wid, (int, float)):
            self.normwid = [wid, wid, wid]
        else:
            assert len(wid) == 3
            assert all([isinstance(v, (type(None), int, float))
                        for v in wid])
            self.normwid = list(wid)
        # if both score or normwid is None => do not use
        for i in range(3):
            if self.score[2*i:2*i+2] == (None, None):
                self.normwid[i] = None
        self.meta = None
        self.bgimage = None
        self.logger = logging.getLogger('FLIR')
        self.datadir = datadir
        self.basetime = basetime
        self.uubnum = uubnum

    def rad2temperature(self, rad):
        """Calculate temperature from radiance and FLIR calibration constants
uses self.meta - dict with FLIR constants (uses R1, B, F)
rad - numpy array with radiance
return numpy array with temperature in deg. C
* suppose emissivity 1.0, no absorption/radiation in air"""
        R1, B = self.meta['PlanckR1'], self.meta['PlanckB']
        F = self.meta['PlanckF']
        temperatureC = B / np.log(R1 / rad + F) - 273.15
        return temperatureC

    def readFFF(self, fn, bg=False):
        """Read content of FLIR .fff file
    fn - filename with fff
    bg - if True, store as bgimage too
    set/check meta
    return numpy.array with calculated radiance, i.e. W/m^2/srad per pixel,
        integrated over wavelengths"""
        cmd = [EXIFTOOL, fn, "-j"]
        res = subprocess.check_output(cmd, universal_newlines=True)
        meta = json.loads(res)
        if isinstance(meta, (tuple, list)) and len(meta) == 1:
            meta = meta[0]

        cmd = [EXIFTOOL, fn, "-b", "-RawThermalImage"]
        res = subprocess.check_output(cmd)
        im = PIL.Image.open(io.BytesIO(res)).transpose(
            PIL.Image.FLIP_LEFT_RIGHT)
        radiance = meta['PlanckR2']*(np.array(im) + meta['PlanckO'])

        if self.meta is None:
            self.meta = {key: meta[key] for key in FlirEval.METAKEYS}
        else:
            assert all([self.meta[key] == meta[key]
                        for key in FlirEval.METAKEYS]), "Meta changed"
        if bg:
            self.bgimage = radiance
        return radiance

    def calcIrad(self, rad):
        """Calculate integrated radiance from components
return dict {label: irad value}"""
        assert self.bgimage is not None, 'No background defined'
        arr = rad - self.bgimage
        irad = {label: arr[pymin:pymax, pxmin:pxmax].sum()
                for label, (pxmin, pymin, pxmax, pymax)
                in self.board.iterComponents(self.MARGINS['IRAD'])}
        return irad

    def calcArad(self, rad):
        """Calculate averaged radiance from components
return dict {label: arad value}"""
        assert self.bgimage is not None, 'No background defined'
        arr = rad - self.bgimage
        arad = {label: arr[pymin:pymax, pxmin:pxmax].mean()
                for label, (pxmin, pymin, pxmax, pymax)
                in self.board.iterComponents(self.MARGINS['ARAD'])}
        return arad

    def calcTemp(self, rad):
        """Calculate averaged temperature increase on components
return dict {label: dtemp value}"""
        assert self.bgimage is not None, 'No background defined'
        arr = self.rad2temperature(rad) - self.rad2temperature(self.bgimage)
        arad = {label: arr[pymin:pymax, pxmin:pxmax].mean()
                for label, (pxmin, pymin, pxmax, pymax)
                in self.board.iterComponents(self.MARGINS['TEMP'])}
        return arad

    def evalFFF(self, rad):
        """Evaluate FLIR image
rad - measured radiance
implicitly depends on: bgimage, complimits, score
return (res, comps_iradm, comps_iradp, comps_aradm, comps_aradp,
        comps_tempm, comps_tempp)
    res - True/False if the image passed/failed
    components_lists: tuple of lists of labels below/above irad/arad/temp"""
        assert self.bgimage is not None, "Background image not available"
        score_iradp = score_iradm = score_aradp = score_aradm = 0.0
        score_tempp = score_tempm = 0.0
        comps_iradp = []
        comps_iradm = []
        comps_aradp = []
        comps_aradm = []
        comps_tempp = []
        comps_tempm = []
        if self.normwid[0]:
            irad = {}
            for label, val in self.calcIrad(rad).items():
                m, s = self.complimits[label][0:2]
                mdif = val - m
                ndif = mdif / s
                if ndif > self.normwid[0]:
                    comps_iradp.append(label)
                    score_iradp += mdif - self.normwid[0]*s
                elif ndif < -self.normwid[0]:
                    comps_iradm.append(label)
                    score_iradm += -mdif - self.normwid[0]*s
                irad[label] = (val, mdif, ndif)
        if self.normwid[1]:
            arad = {}
            for label, val in self.calcArad(rad).items():
                m, s = self.complimits[label][2:4]
                mdif = val - m
                ndif = mdif / s
                if ndif > self.normwid[1]:
                    comps_aradp.append(label)
                    score_aradp += ndif - self.normwid[1]
                elif ndif < -self.normwid[1]:
                    comps_aradm.append(label)
                    score_aradm += -ndif - self.normwid[1]
                arad[label] = (val, mdif, ndif)
        if self.normwid[2]:
            temp = {}
            for label, val in self.calcTemp(rad).items():
                m, s = self.complimits[label][4:6]
                mdif = val - m
                ndif = mdif / s
                if ndif > self.normwid[2]:
                    comps_tempp.append(label)
                    score_tempp += ndif - self.normwid[1]
                elif ndif < -self.normwid[2]:
                    comps_tempm.append(label)
                    score_tempm += -ndif - self.normwid[1]
                temp[label] = (val, mdif, ndif)
        passed = True
        if self.score[0] and score_iradm > self.score[0]:
            passed = False
        if self.score[1] and score_iradp > self.score[1]:
            passed = False
        if self.score[2] and score_aradm > self.score[2]:
            passed = False
        if self.score[3] and score_aradp > self.score[3]:
            passed = False
        if self.score[4] and score_tempm > self.score[4]:
            passed = False
        if self.score[5] and score_tempp > self.score[5]:
            passed = False
        fn = self.datadir + ('/flireval_u%04d' % self.uubnum) + \
            self.basetime.strftime('-%Y%m%d.log')
        columns = []
        prolog = """\
# Evaluation of FLIR image
# UUB: %04d, date %s: %s\n""" % (self.uubnum,
                                 self.basetime.strftime('%Y-%m-%d'),
                                 'PASSED' if passed else 'FAILED')
        with open(fn, 'a') as fp:
            fp.write(prolog)
            if self.normwid[0]:
                fp.write("# integrated radiance: wid = %.1f\n" %
                         self.normwid[0])
                if self.score[0]:
                    fp.write("#    score minus/limit = %6.0f / %6.0f\n" % (
                        score_iradm, self.score[0]))
                if self.score[1]:
                    fp.write("#    score plus/limit  = %6.0f / %6.0f\n" % (
                        score_iradp, self.score[1]))
                columns.extend(('IRAD', 'IRAD diff', 'IRAD norm. diff'))
            if self.normwid[1]:
                fp.write("# averaged radiance: wid = %.1f\n" %
                         self.normwid[1])
                if self.score[2]:
                    fp.write("#    score minus/limit = %6.1f / %6.1f\n" % (
                        score_aradm, self.score[2]))
                if self.score[3]:
                    fp.write("#    score plus/limit  = %6.1f / %6.1f\n" % (
                        score_aradp, self.score[3]))
                columns.extend(('ARAD', 'ARAD diff', 'ARAD norm. diff'))
            if self.normwid[2]:
                fp.write("# temperature: wid = %.1f\n" %
                         self.normwid[2])
                if self.score[4]:
                    fp.write("#    score minus/limit = %6.1f / %6.1f\n" % (
                        score_tempm, self.score[4]))
                if self.score[5]:
                    fp.write("#    score plus/limit  = %6.1f / %6.1f\n" % (
                        score_tempp, self.score[5]))
                columns.extend(('TEMP', 'TEMP diff', 'TEMP norm. diff'))
            fp.write("# columns: %s\n" % ' | '.join(columns))
            for label in sorted(self.complimits.keys()):
                fp.write("%-4s" % label)
                if self.normwid[0]:
                    fp.write("  %7.1f %7.1f %6.2f" % tuple(irad[label]))
                if self.normwid[1]:
                    fp.write("  %7.3f %7.3f %6.2f" % tuple(arad[label]))
                if self.normwid[2]:
                    fp.write("  %6.2f %6.2f %6.2f" % tuple(temp[label]))
                fp.write('\n')
        return (passed, comps_iradm, comps_iradp, comps_aradm, comps_aradp,
                comps_tempm, comps_tempp)


class Board:
    """Implementation of board representation:
 - affine transformation between metric coordinates and FLIR pixels:
    (px, py) = T*(x, y, 1)
 - iteration over components"""
    def __init__(self):
        self.points = {}
        self.components = {}
        self.T = np.array(((1.0, 0.0, 0.0), (0.0, -1.0, 0.0)))
        self.logger = logging.getLogger('Board')

    @staticmethod
    def readPoints(fn):
        """Read point coordinates [in mm or pix] from CSV file
 format: label, x, y
 return dict {label: (x, y)}"""
        points = {}
        with open(fn, 'r') as fp:
            for line in fp:
                if line[0] in ('#', '\n'):
                    continue
                label, x, y = line.split(',')
                points[label] = (float(x), float(y))
        return points

    def readCoordPoints(self, fn):
        self.points.update(Board.readPoints(fn))

    def readComponents(self, fn):
        """Read components coordinates [in mm] from CSV file
 format: label, xmin, ymin, xmax, ymax"""
        coord = {}
        with open(fn, 'r') as fp:
            for line in fp:
                if line[0] in ('#', '\n'):
                    continue
                items = line.split(',')
                label, xmin, ymin, xmax, ymax = items[:5]
                desc = items[5].strip() if len(items) == 6 else None
                coord[label] = (float(xmin), float(ymin),
                                float(xmax), float(ymax), desc)
        self.components.update(coord)

    def calibrate(self, pixpoints):
        """Calculate T matrix matching self.points (in mm) and pixpoints
pixpoints - list of tuples (label, px, py)"""
        xTx = np.zeros((3, 3))
        pTx = np.zeros((2, 3))
        for label, (px, py) in pixpoints.items():
            if label not in self.points:
                self.logger.warning('Point %s not known', label)
                continue
            x, y = self.points[label]
            xvec = np.array((x, y, 1))
            pvec = np.array((px, py))
            xTx += np.outer(xvec, xvec)
            pTx += np.outer(pvec, xvec)
        self.T = np.dot(pTx, np.linalg.inv(xTx))

    def mm2pix(self, x, y):
        xvec = np.array((x, y, 1))
        return np.dot(self.T, xvec).flatten().tolist()

    def iterComponents(self, boundary=0):
        """Iterate over components and provide sub-array slice
boundary - increase area by boundary (in pixels) (decrase if negative)
yield tuple (label, pxmin, pymin, pxmax, pymax)"""
        for label, coord in self.components.items():
            pxmin, pymin = self.mm2pix(*coord[0:2])
            pxmax, pymax = self.mm2pix(*coord[2:4])
            pxmin = round(pxmin - boundary)
            if pxmin < 0:
                pxmin = 0
            elif pxmin >= MAXX:
                pxmin = MAXX-1
            pymin = round(pymin - boundary)
            if pymin < 0:
                pymin = 0
            elif pymin >= MAXY:
                pymin = MAXY-1
            pxmax = round(pxmax + boundary)
            if pxmax < 1:
                pxmax = 1
            elif pxmax > MAXX:
                pxmax = MAXX
            pymax = round(pymax + boundary)
            if pymax < 1:
                pymax = 1
            elif pymax > MAXY:
                pymax = MAXY
            if pxmin >= pxmax:
                pxmax = pxmin+1
            if pymin >= pymax:
                pymax = pymin+1
            yield label, (pxmin, pymin, pxmax, pymax)

    def saveImage(self, array, fn, title='', clim=None, drawComps=None):
        """Save array as image into file
    array - numpy 2D array
    fn - filename to save
    title - optional title
    clim - if present, limit values in the array
    drawComps - dict of components to draw {label: color}
    """
        fig, ax = plt.subplots()
        fig.set_size_inches(16, 10)
        ax.set_axis_off()
        plt.imshow(array, clim=clim, origin='lower')
        if drawComps:
            for label, (pxmin, pymin, pxmax, pymax) in self.iterComponents():
                color = drawComps.get(label, None)
                if color is None:
                    continue
                rect = patches.Rectangle(
                    (pxmin, pymin), pxmax-pxmin, pymax-pymin,
                    linewidth=1, edgecolor=color, facecolor='none')
                ax.add_patch(rect)
                # text = self.components[label][4]
                # if text is None:
                #     text = label
                text = label
                plt.text(pxmax+1, pymin, text, color=color,
                         fontsize=8,
                         horizontalalignment='left',
                         verticalalignment='baseline')
                # pxc = (pxmin+pxmax)/2
                # pyc = (pymin+pymax)/2
                # plt.text(pxc, pyc, text, color=color,
                #          fontsize=8,
                #          horizontalalignment='center',
                #          verticalalignment='center')
        if title:
            ax.set_title(title)
        plt.colorbar()
        plt.savefig(fn, bbox_inches='tight')
        plt.close()
