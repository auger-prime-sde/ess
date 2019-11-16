"""
 ESS procedure
 connector to SDEU DB
"""

import json
import logging

from dataproc import label2item


class DBconnector(object):
    """Connector to SDEU DB"""
    LOGITEMS = ('noise', 'noisestat', 'gain', 'freqgain', 'cutoff')
    PHASES = ('pretest', 'ess', 'burnin', 'final')
    EMPTY_MP = {key: None for key in (
        'timestamp', 'meas_point', 'rel_time', 'set_temp', 'remark')}

    def __init__(self, ctx, dbinfo):
        """Constructor
ctx - context object, used keys: datadir, basetime, phase, tester, uubnums
dbinfo - dict with DB info:
    dbuser, dbpass - credentials
    urlGetUUB - URL to get UUB info
    urlCommit - URL to commit ESS results
    logitems - list of items to log (must be in LOGITEMS)"""
        self.uubnums = ctx.uubnums
        assert ctx.phase in DBconnector.PHASES
        self.dbcred = (dbinfo['dbuser'], dbinfo['dbpass'])
        self.urlGetUUB = dbinfo.get('urlGetUUB', None)
        self.urlCommit = dbinfo.get('urlCommit', None)
        self.logitems = dbinfo['logitems']
        assert all([item in DBconnector.LOGITEMS for item in self.logitems])
        self.logger = logging.getLogger('DBcon')
        self.fp = None
        self.fp = open(ctx.datadir + 'db-%s' % ctx.phase +
                       ctx.basetime.strftime('-%Y%m%d%H%M.json'), 'w')
        self.fp.write(
            '{ "typ": "run", "phase": "%s", "tester": "%s", "date": "%s" }\n'
            % (ctx.phase, ctx.tester, ctx.basetime.strftime('%Y-%m-%d')))
        self.measpoint = DBconnector.EMPTY_MP.copy()
        self.measrecs = []

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
        json.dump(self.measpoint, self.fp)
        self.fp.write('\n')
        for rec in self.measrecs:
            json.dump(rec, self.fp)
            self.fp.write('\n')
        self.measpoint = DBconnector.EMPTY_MP.copy()
        self.measrecs = []

    def queryInternalSN(self, uubnum):
        """Query Internal SN from DB"""
        return '00-11-22-33-44-55'

    def getLogHandler(self, logitem, **kwargs):
        """Return LogHandler for item"""
        assert logitem in self.logitems
        flabels = kwargs.get('flabels', None)
        return LogHandlerDB(logitem, self, self.uubnums, flabels)

    def commit(self):
        """Commit logged records to DB"""
        pass


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
