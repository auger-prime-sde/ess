"""
 ESS procedure
 connector to SDEU DB
"""

import os
import json
import logging
import ssl
import random
import string
import mmap
from http.client import HTTPSConnection

from dataproc import label2item
from UUB import VIRGINUUBNUM


class DBconnector(object):
    """Connector to SDEU DB"""
    LOGITEMS = ('noise', 'noisestat', 'gain', 'freqgain', 'cutoff')
    PHASES = ('pretest', 'ess', 'burnin', 'final')
    EMPTY_MP = {key: None for key in (
        'timestamp', 'meas_point', 'rel_time', 'set_temp', 'remark')}
    EMPTY_MP['typ'] = 'measpoint'

    def __init__(self, ctx, dbinfo):
        """Constructor
ctx - context object, used keys: datadir, basetime, phase, tester, uubnums
dbinfo - dict with DB info:
    host_addr - DNS name or IP address of a server to connect
    host_port - (HTTPS) port of the server to connect
    server_cert - server's certificate
    client_key - client's private key
    client_cert - client's certificate
    urlSN - URL to get UUB internal SN
    urlCommit - URL to commit ESS results
    logitems - list of items to log (must be in LOGITEMS)"""
        self.CHUNKSIZE = 50*1024  # size of file chunk for HTTP POST
        self.uubnums = ctx.uubnums
        assert ctx.phase in DBconnector.PHASES
        self.dbinfo = dbinfo
        self.logitems = dbinfo['logitems']
        assert all([item in DBconnector.LOGITEMS for item in self.logitems])
        self.logger = logging.getLogger('DBcon')
        dbfn = (ctx.datadir + 'db-%s' % ctx.phase +
                ctx.basetime.strftime('-%Y%m%d%H%M.json'))
        self.files = [('dbjs', dbfn)]
        self.fp = open(dbfn, 'w')
        self.fp.write(
            '{"typ": "run", "phase": "%s", "tester": "%s", "date": "%s"}\n'
            % (ctx.phase, ctx.tester, ctx.basetime.strftime('%Y-%m-%d')))
        self.measpoint = DBconnector.EMPTY_MP.copy()
        self.measrecs = []
        self.sslctx = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH, cafile=dbinfo['server_cert'])
        self.sslctx.load_cert_chain(certfile=dbinfo['client_cert'],
                                    keyfile=dbinfo['client_key'])

    def __del__(self):
        self.close()

    def close(self):
        if self.fp is not None:
            self._write_measrecords()
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

    def attach(self, name, filename, description=None, preview=False,
               uubs=None, run=True, fieldname=None):
        """Attach a file to run/uub
name - shortname of the attachment
filename - file to attach
description - optional description
preview - if True, generate preview for an image
uubs - list of UUB numbers of UUBs in the run to link to
run - if True, link to ESSrun
fieldname - unique name for HTTP POST transport. Use name if None
raise exception if something wrong"""
        with open(filename, 'r'):
            pass  # check that the filename is readable
        if fieldname is None:
            fieldname = name
        assert fieldname not in [item[0] for item in self.files]  # uniqueness
        if uubs is None:
            uubs = ()
        else:
            assert set(uubs) <= set(self.uubnums)
        self.files.append((fieldname, filename))
        d = {'typ': 'attach',
             'name': name,
             'description': description,
             'preview': preview,
             'uubs': uubs,
             'fieldname': fieldname}
        json.dump(d, self.fp)
        self.fp.write('\n')

    def queryInternalSN(self, uubnums=None):
        """Query Internal SN from DB for uubnums (use self.uubnums if None)
uubnums - tuple/list of UUB numbers
return dict {uubnum: '0123456789ab'}"""
        if uubnums is None:
            uubnums = self.uubnums
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

    def commit(self):
        """Commit logged records to DB"""
        self.logger.debug('Commiting')
        self.close()
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
        self.logger.debug("Received data %s", repr(resp.read()))
        conn.close()

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
        assert logitem in self.logitems
        flabels = kwargs.get('flabels', None)
        return LogHandlerDB(logitem, self, self.uubnums, flabels)
    

class LogHandlerDB(object):
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
        elif logitem == 'noisestat':
            self.skiprec = lambda d: 'db_noisestat' not in d
            self.typs = ('pedemean', 'pedestdev', 'noisemean', 'noisestdev')
            self.typemap = {'pedemean': 'pede', 'pedestdev': 'pedesig',
                            'noisemean': 'noise', 'noisestdev': 'noisesig'}
        elif logitem == 'gain':
            self.skiprec = lambda d: 'db_pulse' not in d
            self.typs = ('gain', )
        elif logitem in ('freqgain', 'cutoff'):
            self.skiprec = lambda d: 'db_freq' not in d
            self.typs = (logitem, )
        if logitem == 'freqgain':
            self.flabels = flabels
            self.typs = ('fgain', )
            self.item2key = lambda item: (item['uubnum'], item['flabel'])
        elif logitem in ('noise', 'noisestat'):
            self.item2key = lambda item: (item['uubnum'], item['typ'])
        else:
            self.item2key = lambda item: item['uubnum']

    def __del__(self):
        pass

    def write_rec(self, d):
        if self.skiprec(d):
            return
        if d['timestamp'] != self.dbcon.measpoint['timestamp']:
            self.dbcon._write_measrecords()
        self.dbcon.measpoint.update(
            {key: d[key] for key in DBconnector.EMPTY_MP
             if d.get(key, None) is not None and
             self.dbcon.measpoint[key] is None})
        if self.logitem in ('gain', 'cutoff'):
            values = {uubnum: [None] * 11
                      for uubnum in self.uubnums}
        elif self.logitem in ('noise', 'noisestat'):
            values = {(uubnum, typ): [None] * 11
                      for uubnum in self.uubnums
                      for typ in self.typs}
        elif self.logitem == 'freqgain':
            values = {(uubnum, flabel): [None] * 11
                      for uubnum in self.uubnums
                      for flabel in self.flabels}
        for label, value in d.items():
            item = label2item(label)
            if item.get('typ', None) not in self.typs:
                continue
            key = self.item2key(item)
            if key in values:
                values[key][item['chan']] = value
        if self.logitem in ('noise', 'noisestat'):
            for uubnum in self.uubnums:
                for typ in self.typs:
                    if not all([value is None
                                for value in values[(uubnum, typ)]]):
                        rec = {'typ': self.typemap[typ],
                               'mp': d['meas_point'],
                               'uubnum': uubnum,
                               'values': values[(uubnum, typ)][1:]}
                        self.dbcon.measrecs.append(rec)
        elif self.logitem == 'freqgain':
            for uubnum in self.uubnums:
                for flabel in self.flabels:
                    if not all([value is None
                                for value in values[(uubnum, flabel)]]):
                        rec = {'typ': 'freqgain',
                               'mp': d['meas_point'],
                               'uubnum': uubnum,
                               'flabel': flabel,
                               'values': values[(uubnum, flabel)][1:]}
                        self.dbcon.measrecs.append(rec)
        else:
            for uubnum in self.uubnums:
                if not all([value is None for value in values[uubnum]]):
                    rec = {'typ': self.typs[0],
                           'mp': d['meas_point'],
                           'uubnum': uubnum,
                           'values': values[uubnum][1:]}
                    self.dbcon.measrecs.append(rec)
