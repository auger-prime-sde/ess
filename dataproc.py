"""
 ESS procedure
 data processor
"""

import threading
import logging
import itertools
from time import sleep
from Queue import Empty
import numpy

import hsfitter

class DataProcessor(threading.Thread):
    """Generic data processor"""
    id_generator = itertools.count()
    stop = threading.Event()
    timeout = 1.0
    workhorses = []
    def __init__(self, q_dp):
        super(DataProcessor, self).__init__()
        self.myid = next(self.id_generator)
        self.q_dp = q_dp
        logger = logging.getLogger('DP%d' % self.myid)
        logger.debug('init finished')

    def run(self):
        logger = logging.getLogger('DP%d' % self.myid)
        while not self.stop.is_set():
            try:
                item = self.q_dp.get(True, self.timeout)
            except Empty:
                continue
            logger.debug('processing %s', item2label(item))
            for wh in self.workhorses:
                wh.calculate(item)
        logger.info('run finished')

def item2label(item, **kwargs):
    """Construct label/name for q_resp from item"""
    attr = []
    if 'uubnum' in item:
        attr.append('u%04d' % item['uubnum'])
    if 'chan' in kwargs:
        # transform chan 10 -> c0
        attr.append('c%d' % (kwargs['chan'] % 10))
    if 'volt' in item:
        attr.append('v%02d' % int(item['volt'] * 10.))
    return '_'.join(attr)

class DP_pede(object):
    """Data processor workhorse to calculate pedestals"""
    # parameters
    BINSTART = 50
    BINEND = 550
    def __init__(self, q_resp, item2label):
        """Constructor.
q_resp - a logger queue
item2label - a function to generate names
"""
        self.q_resp = q_resp
        self.item2label = item2label

    def calculate(self, item):
        array = item['yall'][self.BINSTART:self.BINEND,:]
        mean = array.mean(axis=0)
        stddev = array.std(axis=0)
        res = {'timestamp': item['timestamp']}
        if 'meas.point' in item:
            res['meas_point'] = item['meas.point']
        for ch, (m, s) in enumerate(zip(mean, stddev)):
            label = self.item2label(item, chan=ch+1)
            res['pede_' + label] = m
            res['pedesig_' + label] = s
        self.q_resp.put(res)
                                 
class DP_hsampli(object):
    """Data processor workhorse to calculate amplitude of half-sines"""
    # parameters
    def __init__(self, q_resp, item2label, w):
        """Constructor.
q_resp - a logger queue
item2label - a function to generate names
w - width of half-sine in us
"""
        self.q_resp = q_resp
        self.item2label = item2label
        self.hsf = hsfitter.HalfSineFitter(w)

    def calculate(self, item):
        hsfres = self.hsf.fit(item['yall'], hsfitter.AMPLI)
        res = {'timestamp': item['timestamp']}
        if 'meas.point' in item:
            res['meas_point'] = item['meas.point']
        for ch, ampli in enumerate(hsfres['ampli']):
            label = self.item2label(item, chan=ch+1)
            res['ampli_' + label] = ampli
        self.q_resp.put(res)
                                 
