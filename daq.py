#!/usr/bin/env python

"""
   Standalone UUB data acquistion program
   Petr Tobiska <tobiska@fzu.cz>
"""

import os
import sys
import json
import logging
import time
from datetime import datetime
from Queue import Queue

# ESS stuff
from UUB import UUBlisten, UUBconvData, UUBtelnet
from dataproc import DataProcessor, DP_store

VERSION = '20190124'


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
        self.q_ndata = Queue()
        self.q_dp = Queue()

        self.uubnums = [int(uubnum) for uubnum in d['uubnums']]
        # UUBs - UUBdaq and UUBlisten
        self.ulisten = UUBlisten(self.q_ndata)
        self.ulisten.permanent = True
        self.ulisten.uubnums = set(self.uubnums)
        self.ulisten.start()
        self.uconv = UUBconvData(self.q_ndata, self.q_dp)
        self.uconv.start()

        # UUBs - UUBtelnet
        self.telnet = UUBtelnet(None, *self.uubnums)
        self.telnet.login()

        # data processing
        self.dp0 = DataProcessor(self.q_dp)
        self.dp0.workhorses.append(DP_store(self.datadir))
        self.dp0.start()

    def stop(self):
        """Stop all threads"""
        self.telnet.logout()
        self.dp0.stop.set()
        self.ulisten.stop.set()
        self.uconv.stop.set()


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
