#!/usr/bin/env python

"""
   Standalone UUB data acquistion program
   Petr Tobiska <tobiska@fzu.cz>
"""

import os
import sys
import json
import logging
import logging.config
import logging.handlers
import multiprocessing
import time
from datetime import datetime
import queue

# ESS stuff
from UUB import UUBlisten, UUBtelnet
from dataproc import DataProcessor
from logger import QueDispatch, QLogHandler

VERSION = '20210531'


class DAQ(object):
    """ DAQ process implementation """

    def __init__(self, js):
        if hasattr(js, 'read'):
            d = json.load(js)
        else:
            d = json.loads(js)

        # datadir
        self.datadir = datetime.now().strftime(
            d.get('datadir', 'data-%Y%m%d/'))
        if self.datadir[-1] != '/':
            self.datadir += '/'
        if not os.path.isdir(self.datadir):
            os.mkdir(self.datadir)

        if 'comment' in d:
            with open(self.datadir + 'README.txt', 'w') as f:
                f.write(d['comment'] + '\n')

        if 'logging' in d:
            kwargs = {key: d['logging'][key]
                      for key in ('level', 'format', 'filename')
                      if key in d['logging']}
            if 'filename' in kwargs:
                kwargs['filename'] = datetime.now().strftime(
                    kwargs['filename'])
                if kwargs['filename'][0] not in ('.', '/'):
                    kwargs['filename'] = self.datadir + kwargs['filename']
            logging.basicConfig(**kwargs)

        # queues
        self.q_ndata = multiprocessing.JoinableQueue()
        self.q_dpres = multiprocessing.Queue()
        self.q_log = multiprocessing.Queue()
        self.q_resp = queue.Queue()
        # manager for shared dict for invalid channels
        self.mgr = multiprocessing.Manager()
        self.invalid_chs_dict = self.mgr.dict()

        self.qlistener = logging.handlers.QueueListener(
            self.q_log, QLogHandler())
        self.qlistener.start()

        # UUB channels
        self.chans = d.get('chans', range(1, 11))

        # start DataProcessors before anything is logged, otherwise child
        # processes may lock at acquiring lock to existing log handlers
        dp_ctx = {'q_ndata': self.q_ndata,
                  'q_resp': self.q_dpres,
                  'q_log': self.q_log,
                  'inv_chs_dict': self.invalid_chs_dict,
                  'datadir': self.datadir,
                  'splitmode': None,
                  'chans': self.chans}
        self.n_dp = d.get('n_dp', multiprocessing.cpu_count() - 2)
        self.dataprocs = [multiprocessing.Process(
            target=DataProcessor, name='DP%d' % i, args=(dp_ctx, ))
                          for i in range(self.n_dp)]
        for dp in self.dataprocs:
            dp.start()
        self.qdispatch = QueDispatch(self.q_dpres, self.q_resp, zLog=False)
        self.qdispatch.start()

        self.uubnums = [int(uubnum) for uubnum in d['uubnums']]
        # UUBs - UUBdaq and UUBlisten
        self.ulisten = UUBlisten(self.q_ndata)
        self.ulisten.permanent = True
        self.ulisten.uubnums = set(self.uubnums)
        self.ulisten.start()

        # UUBs - UUBtelnet
        telnetcmds = d.get('telnetcmds', None)
        if telnetcmds is not None:
            self.telnet = UUBtelnet(None, *self.uubnums)
            self.telnet._runcmds(map(str, telnetcmds))
        else:
            self.telnet = None

    def stop(self):
        """Stop all threads"""
        self.ulisten.stop.set()
        for i in range(self.n_dp):
            self.q_ndata.put(None)
        if self.q_dpres is not None:
            self.q_dpres.put(None)
        # join all threads
        self.qlistener.stop()
        self.ulisten.join()
        if self.telnet is not None:
            self.telnet.join()
        self.qdispatch.join()
        # join DP processes
        for dp in self.dataprocs:
            dp.join()
        self.mgr.shutdown()


if __name__ == '__main__':
    try:
        with open(sys.argv[1], 'r') as fp:
            print("Starting DAQ")
            daq = DAQ(fp)
    except (IndexError, IOError, ValueError):
        print("Usage: %s <JSON config file>" % sys.argv[0])
        raise

    try:
        print("DAQ running, press Ctrl-C to stop")
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("Quitting DAQ")
        daq.stop()
