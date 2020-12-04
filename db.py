"""
 ESS procedure
 connector to SDEU DB
"""

import os
import json
import logging
import queue
import ssl
import random
import string
import mmap
from http.client import HTTPSConnection

from dataproc import label2item, item2label
from UUB import VIRGINUUBNUM


class DBconnector:
    """Connector to SDEU DB"""
    LOGITEMS = ('ramp', 'noise', 'noisestat', 'gain', 'freqgain', 'cutoff',
                'voltramp')
    PHASES = ('pretest', 'ess', 'burnin', 'combo')
    EMPTY_MP = {key: None for key in (
        'timestamp', 'meas_point', 'rel_time', 'set_temp', 'remark')}
    EMPTY_MP['typ'] = 'measpoint'

    def __init__(self, ctx, dbinfo, log=True):
        """Constructor
ctx - context object (i.e. ESS), used keys:
    datadir, basetime, phase, tester, uubnums, q_att, starttime
dbinfo - dict with DB info:
    host_addr - DNS name or IP address of a server to connect
    host_port - (HTTPS) port of the server to connect
    server_cert - server's certificate
    client_key - client's private key
    client_cert - client's certificate
    urlSN - URL to get UUB internal SN
    urlCommit - URL to commit ESS results
log - if True, write log recores"""
        self.CHUNKSIZE = 50*1024  # size of file chunk for HTTP POST
        self.ctx = ctx
        assert ctx.phase in DBconnector.PHASES
        self.dbinfo = dbinfo
        self.logger = logging.getLogger('DBcon')
        self.evaluators = None
        if log:
            dbfn = (ctx.datadir + 'db-%s' % ctx.phase +
                    ctx.basetime.strftime('-%Y%m%d%H%M.json'))
            self.files = [('dbjs', dbfn)]
            self.fp = open(dbfn, 'w')
            self.measpoint = DBconnector.EMPTY_MP.copy()
            self.measrecs = []
        else:
            self.fp = None
            self.files = None
        self.sslctx = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH, cafile=dbinfo['server_cert'])
        self.sslctx.load_cert_chain(certfile=dbinfo['client_cert'],
                                    keyfile=dbinfo['client_key'])

    def start(self):
        """Write <run> record, ctx.starttime must be defined"""
        if self.fp is None:
            return
        self.fp.write('{"typ": "run", "phase": "%s", "tester": "%s", ' %
                      (self.ctx.phase, self.ctx.tester))
        self.fp.write('"starttime": "%s"}\n' %
                      self.ctx.starttime.strftime('%Y-%m-%dT%H:%M:%S'))

    def __del__(self):
        self.close()

    def close(self):
        if self.fp is not None:
            self._write_measrecords()
            self.process_qatt()
            if self.evaluators is not None:
                self.write_summary()
            self.fp.close()
            self.fp = None

    def _write_measrecords(self):
        if self.measpoint['timestamp'] is None:
            return
        self.logger.debug(
            'Dumping ts %s, %d records',
            self.measpoint['timestamp'].strftime("%Y%m%d %H:%M:%S"),
            len(self.measrecs))
        del self.measpoint['timestamp']
        self.measpoint['index'] = self.measpoint.pop('meas_point')
        json.dump(self.measpoint, self.fp)
        self.fp.write('\n')
        for rec in self.measrecs:
            json.dump(rec, self.fp)
            self.fp.write('\n')
        self.measpoint = DBconnector.EMPTY_MP.copy()
        self.measrecs = []

    def write_summary(self):
        """Generate summary records
evaluators: dict{ typ: eval} -> calls eval(uubnum) to get summary"""
        self.logger.info('writing summary')
        uubnums = [uubnum for uubnum in self.ctx.uubnums
                   if uubnum is not None and uubnum != VIRGINUUBNUM]
        for uubnum in uubnums:
            d = {'typ': 'summary',
                 'uubnum': uubnum}
            for typ, evalit in self.evaluators.items():
                d[typ] = evalit.summary(uubnum)
            json.dump(d, self.fp)
            self.fp.write('\n')
        self.evaluators = None

    def attach(self, name, filename, description=None, uubs=None,
               run=True, fieldname=None):
        """Attach a file to run/uub
name - shortname of the attachment
filename - file to attach
description - optional description
preview - if True, generate preview for an image
uubs - list of UUB numbers of UUBs in the run to link to
run - if True, link to ESSrun
fieldname - unique name for HTTP POST transport. Use name if None
raise exception if something wrong"""
        if self.files is None:
            return
        with open(filename, 'r'):
            pass  # check that the filename is readable
        if fieldname is None:
            fieldname = name
        assert fieldname not in [item[0] for item in self.files]  # uniqueness
        if uubs is None:
            uubs = ()
        else:
            assert set(uubs) <= set(self.ctx.uubnums)
        self.files.append((fieldname, filename))
        d = {'typ': 'attach',
             'name': name,
             'description': description,
             'uubs': uubs,
             'run': run,
             'fieldname': fieldname}
        json.dump(d, self.fp)
        self.fp.write('\n')

    def queryInternalSN(self, uubnums=None):
        """Query Internal SN from DB for uubnums (use self.uubnums if None)
uubnums - tuple/list of UUB numbers
return dict {uubnum: '0123456789ab'}"""
        if uubnums is None:
            uubnums = [uubnum for uubnum in self.ctx.uubnums
                       if uubnum is not None and uubnum != VIRGINUUBNUM]
        if not uubnums:
            return {}
        assert all([0 < uubnum < VIRGINUUBNUM for uubnum in uubnums])
        param = '&'.join(['uubnum=%d' % uubnum for uubnum in uubnums])
        self.logger.debug('Acquiring internal SN')
        conn = HTTPSConnection(self.dbinfo['host_addr'],
                               port=self.dbinfo['host_port'],
                               context=self.sslctx)
        # conn.set_debuglevel(3)
        conn.request("GET", self.dbinfo['urlSN'] + param)
        resp = conn.getresponse()
        self.logger.debug('Received status %d', resp.status)
        data = resp.read()
        conn.close()
        d = json.loads(data)
        self.logger.info('Received data: %s', repr(d))
        return d

    def process_qatt(self):
        """Process attachment records from q_att"""
        assert self.ctx.q_att.empty() or self.fp is not None, \
            'q_att not empty but fp already closed'
        while True:
            try:
                rec = self.ctx.q_att.get(False)
            except queue.Empty:
                break
            if 'name' not in rec or 'filename' not in rec:
                self.logger.error('Missing name or filename in attachment %s',
                                  repr(rec))
            else:
                name = rec.pop('name')
                filename = rec.pop('filename')
                self.logger.info('Attaching %s -> %s', name, filename)
                self.attach(name, filename, **rec)

    def commit(self):
        """Commit logged records to DB
return True/False if successful or failure"""
        if self.files is None:
            return False
        if not self.ctx.q_att.empty():
            if self.fp is not None:
                self.process_qatt()
            else:
                self.logger.error(
                    'Pre-commit: q_att not empty but fp already closed')
        if self.evaluators is not None:
            if self.fp is not None:
                self.write_summary()
            else:
                self.logger.error(
                    'Summary not written and fp already closed')
        self.close()  # in case not closed yet
        self.logger.debug('Commiting to DB')
        self._boundary()
        conn = HTTPSConnection(self.dbinfo['host_addr'],
                               port=self.dbinfo['host_port'],
                               context=self.sslctx)
        headerCT = b'multipart/form-data; boundary="%s"' % self.boundary
        headerCL = b'%d' % self._contentLength()
        conn.request("POST", self.dbinfo['urlCommit'], body=self._body(),
                     headers={'Content-Type': headerCT,
                              'Content-Length': headerCL})
        resp = conn.getresponse()
        self.logger.debug('Received status %d', resp.status)
        if resp.status != 204:
            fnerr = self.ctx.datadir + \
                    self.ctx.basetime.strftime('db-error-%Y%m%d%H%M.html')
            with open(fnerr, 'wb') as fperr:
                fperr.write(resp.read())
        # self.logger.debug("Received data %s", repr(resp.read()))
        conn.close()
        return resp.status == 204

    def _boundary(self):
        """Generate boundary suitable for db.js and attachments"""
        boundaryLen = 20
        while True:
            boundary = bytes(''.join(random.choices(string.ascii_uppercase +
                                                    string.ascii_lowercase +
                                                    string.digits,
                                                    k=boundaryLen)), 'ascii')
            collision = False
            for fieldname, filename in self.files:
                with open(filename, 'rb') as fp, \
                     mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ) as s:
                    if s.find(boundary) != -1:
                        collision = True
                        break
            if not collision:
                self.boundary = boundary
                break

    def _body(self):
        for name, fn in self.files:
            bname = bytes(name, 'ascii')
            yield (
                b'--%s\r\n' +
                b'Content-Disposition: form-data; name=%s;' +
                b' filename=%s\r\n\r\n') % (self.boundary, bname, bname)
            with open(fn, 'rb') as fp:
                while True:
                    data = fp.read(self.CHUNKSIZE)
                    if not data:
                        break
                    yield data
            yield b'\r\n'
        yield b'--%s--\r\n' % self.boundary

    def _contentLength(self):
        """Calculate Content-Length based on size of file and fieldnames"""
        seplen = 58 + len(self.boundary)
        nfile = len(self.files)
        clen = seplen*nfile + 2*sum([len(f[0]) for f in self.files])
        clen += 6 + len(self.boundary)  # trailer boundary
        clen += sum([os.stat(f[1]).st_size for f in self.files])
        return clen

    def getLogHandler(self, logitem, **kwargs):
        """Return LogHandler for item"""
        flabels = kwargs.get('flabels', None)
        return LogHandlerDB(logitem, self, self.ctx.uubnums, flabels)


class LogHandlerDB:
    def __init__(self, logitem, dbcon, uubnums, flabels=None):
        """Constructor.
logitem - item to log, from DBconnector.LOGITEMS
dbcon - instance of DBconnector, for measpoint, measrecs and _write_measrecords
uubnums - list of UUBnums to log
flabels - list of frequencies to log for freqgain
"""
        assert logitem in DBconnector.LOGITEMS
        self.logitem, self.dbcon, self.uubnums = logitem, dbcon, uubnums
        if logitem == 'noise':
            self.skiprec = lambda d: 'db_noise' not in d
            self.typs = ('pede', 'noise')
            self.typemap = {'pede': 'pede', 'noise': 'noise'}
            self.evaltyp = ('', 'N')
        elif logitem == 'noisestat':
            self.skiprec = lambda d: 'db_noisestat' not in d
            self.typs = ('pedemean', 'pedestdev', 'noisemean', 'noisestdev')
            self.typemap = {'pedemean': 'pede', 'pedestdev': 'pedesig',
                            'noisemean': 'noise', 'noisestdev': 'noisesig'}
            self.evaltyp = ('evalnoise', 'N')
        elif logitem == 'gain':
            self.skiprec = lambda d: 'db_pulse' not in d
            self.typs = ('gain', )
            self.evaltyp = ('evalpulse', 'P')
        elif logitem == 'freqgain':
            self.skiprec = lambda d: 'db_freq' not in d
            self.typs = ('fgain', )
            self.flabels = flabels
            self.evaltyp = ('evalfgain', 'F')
        elif logitem == 'cutoff':
            self.skiprec = lambda d: 'db_freq' not in d
            self.typs = ('cutoff', )
            self.evaltyp = ('evalcutoff', 'F')
        elif logitem == 'ramp':
            self.skiprec = lambda d: 'db_ramp' not in d
        elif logitem == 'voltramp':
            self.skiprec = lambda d: 'volt_ramp' not in d

        # item2key
        if logitem == 'freqgain':
            self.item2key = lambda item: (item['uubnum'], item['flabel'])
        elif logitem in ('noise', 'noisestat'):
            self.item2key = lambda item: (item['uubnum'], item['typ'])
        else:
            self.item2key = lambda item: item['uubnum']
        # item2ekey
        if logitem == 'noisestat':
            self.item2ekey = lambda item: item['uubnum']
        else:
            self.item2ekey = self.item2key
        self.label = 'LogHandlerDB:' + logitem

    def write_rec(self, d):
        if self.skiprec(d):
            return
        if d['timestamp'] != self.dbcon.measpoint['timestamp']:
            self.dbcon._write_measrecords()
        self.dbcon.measpoint.update(
            {key: d[key] for key in DBconnector.EMPTY_MP
             if d.get(key, None) is not None and
             self.dbcon.measpoint[key] is None})
        uubnums = [uubnum for uubnum in self.uubnums if uubnum is not None]
        if self.logitem in ('gain', 'cutoff'):
            values = {uubnum: [None] * 11 for uubnum in uubnums}
        elif self.logitem in ('noise', 'noisestat'):
            values = {(uubnum, typ): [None] * 11
                      for uubnum in uubnums
                      for typ in self.typs}
        elif self.logitem == 'freqgain':
            values = {(uubnum, flabel): [None] * 11
                      for uubnum in uubnums
                      for flabel in self.flabels}
        elif self.logitem == 'ramp':
            for uubnum in uubnums:
                label = item2label(typ='rampdb', uubnum=uubnum)
                rec = {'typ': 'ramp',
                       'mp': d['meas_point'],
                       'uubnum': uubnum,
                       'result': d[label]}
                self.dbcon.measrecs.append(rec)
            return  # fast track
        elif self.logitem == 'voltramp':
            vrtyp = ''.join(d['volt_ramp'])
            labeltemplate = 'voltramp' + vrtyp + '_u%04d'
            elabeltemplate = 'evalpon' + vrtyp + '_u%04d'
            for uubnum in uubnums:
                label = labeltemplate % uubnum
                elabel = elabeltemplate % uubnum
                rec = {'typ': 'voltramp',
                       'mp': d['meas_point'],
                       'uubnum': uubnum,
                       'vrtyp': vrtyp,
                       'voltage': d.get(label, None),
                       'eval': d.get(elabel, None)}
                self.dbcon.measrecs.append(rec)
            return  # fast track

        # prepare evals
        if self.logitem == 'freqgain':
            evals = {(uubnum, flabel): [None] * 11
                     for uubnum in uubnums
                     for flabel in self.flabels}
        else:
            evals = {uubnum: [None] * 11 for uubnum in uubnums}

        for label, value in d.items():
            item = label2item(label)
            typ = item.get('typ', None)
            ftyp = item.get('functype', None)
            if typ in self.typs:
                key = self.item2key(item)
                if key in values:
                    values[key][item['chan']] = value
            elif (typ, ftyp) == self.evaltyp:
                key = self.item2ekey(item)
                if key in evals:
                    evals[key][item['chan']] = value
        if self.logitem in ('noise', 'noisestat'):
            for uubnum in uubnums:
                ieval = LogHandlerDB.eval2int(evals[uubnum][1:])
                for typ in self.typs:
                    if not all([value is None
                                for value in values[(uubnum, typ)]]):
                        rec = {'typ': self.typemap[typ],
                               'mp': d['meas_point'],
                               'uubnum': uubnum,
                               'values': values[(uubnum, typ)][1:],
                               'eval': ieval}
                        self.dbcon.measrecs.append(rec)
        elif self.logitem == 'freqgain':
            for uubnum in uubnums:
                for flabel in self.flabels:
                    if not all([value is None
                                for value in values[(uubnum, flabel)]]):
                        ieval = LogHandlerDB.eval2int(
                            evals[(uubnum, flabel)][1:])
                        rec = {'typ': 'freqgain',
                               'mp': d['meas_point'],
                               'uubnum': uubnum,
                               'flabel': flabel,
                               'values': values[(uubnum, flabel)][1:]}
                        if not all([res is None
                                    for res in evals[(uubnum, flabel)]]):
                            rec['eval'] = evals[(uubnum, flabel)][1:]
                        self.dbcon.measrecs.append(rec)
        else:
            for uubnum in uubnums:
                if not all([value is None for value in values[uubnum]]):
                    ieval = LogHandlerDB.eval2int(evals[uubnum][1:])
                    rec = {'typ': self.typs[0],
                           'mp': d['meas_point'],
                           'uubnum': uubnum,
                           'values': values[uubnum][1:],
                           'eval': ieval}
                    self.dbcon.measrecs.append(rec)

    @staticmethod
    def eval2int(reslist):
        """Convert eval. result to 3bit integer
reslist - tuple of 10 results (None/True/False)
return integer value for DB"""
        EVAL2INT = {None: 0, True: 1, False: 2}
        return sum([EVAL2INT[res] << (3*i)
                    for i, res in enumerate(reslist)])

    def stop(self):
        pass
