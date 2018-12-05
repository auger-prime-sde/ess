"""
  ESS procedure
  Calibrate AFG/cabels/splitter path
"""

import sys
import os
import json
import logging
from datetime import datetime
from time import sleep
import numpy as np
# ESS stuff
from afg import AFG, splitter_amplification
from mdo import MDO
from UUB import gener_funcparams
from dataproc import item2label, label2item, float2expo
from hsfitter import SineFitter

try:
    with open(sys.argv[1], 'r') as fp:
        d = json.load(fp)
except (IndexError, IOError, ValueError):
    print("Usage: %s <JSON config file>" % sys.argv[0])
    raise

# timeouts
TOUT_PREP = 0.4   # delay between afg setting and trigger in s
TOUT_DAQ = 0.1    # timeout between trigger and oscilloscope readout

datadir = datetime.now().strftime(d.get('datadir', './'))
if not os.path.isdir(datadir):
    os.mkdir(datadir)

if 'comment' in d:
    with open(datadir + 'README.txt', 'w') as f:
        f.write(d['comment'] + '\n')

if 'logging' in d:
    kwargs = {key: d['logging'][key]
              for key in ('level', 'format', 'filename')
              if key in d['logging']}
    if 'filename' in kwargs:
        kwargs['filename'] = datetime.now().strftime(kwargs['filename'])
    logging.basicConfig(**kwargs)
logger = logging.getLogger('calib')

afg = AFG(tmcid=d['usbtmc']['afg'], functype='F')
mdo = MDO(tmcid=d['usbtmc']['mdo'])
for item in d['setup']:
    mdo.send(item)

generF = [rec[2] for rec in gener_funcparams() if rec[1] == 'F'][0]
afgkwargs = d.get('afgkwargs', {})

dataslice = d['dataslice'] if 'dataslice' in d else None

# oscilloscope channels with signals
cho = d.get('cho')
ch9 = d.get('ch9')
ch9ampli = d.get('ch9ampli')

# sampling frequency and number of datapoints after dataslice
mdo.send('HEADER 0')
resp = mdo.send('HORIZONTAL:SCALE?', resplen=100)
horscale = float(resp.rstrip())
resp = mdo.send('HORIZONTAL:RECORDLENGTH?', resplen=100)
N = int(resp.rstrip())
FREQ = 1e-6 * N/10 / horscale  # in MHz
if dataslice is not None:
    start, stop, step = dataslice
    if start is None:
        start = 0
    if stop is None:
        stop = N
    if step is None:
        step = 1
    N = (stop - start)/step
    FREQ /= step
logger.debug('N = %d, FREQ = %f, dataslice: %d:%d:%d',
             N, FREQ, *dataslice)
nharm = d.get('nharm', 1)
sf = SineFitter(N=N, FREQ=FREQ, NHARM=nharm)
sf.crop = False

datalog = open(datadir + 'datalog.txt', 'w')
prolog = """\
# calibration of splitter: trace fit results
# columns: flabel | freq[MHz] | ch2 | input voltage[V] |
#          ampli_o[V] | ampli_9[V] | chi_o[V] | chi_9[V] |
#          cos/sin coeffs[V]: n*omega, n = 1..%d
#          vandermont coeffs: x**k, k = 0..%d
#              x = (2*t + 1)/N - 1, t = 0..N - timebin
""" % (sf.NHARM, sf.NPOLY)
datalog.write(prolog)
dlog_formstr = ("%-4s %5.2f  %d %4.2f" +
                "   %7.5f %7.5f   %7.5f %7.5f" +
                " %8.5f"*(2*sf.NHARM + 1 + sf.NPOLY) + "   " +
                " %8.5f"*(2*sf.NHARM + 1 + sf.NPOLY) + "\n")

fitlog = open(datadir + 'fitlog.txt', 'w')
prolog = """\
# calibration of splitter: linearity fit
# columns: flabel | freq[MHz] | ch2 | senso | sens9 | 1-rho_o | 1-rho_9
"""
fitlog.write(prolog)
fit_formstr = "%-4s %5.2f  %d   %7.5f %7.5f  %7.5f %7.5f\n"

# run measurement for all parameters
calibdata = {}
afg.switchOn(True)
for afg_dict, item_dict in generF(**afgkwargs):
    logger.debug("params %s", repr(item_dict))
    afg.setParams(**afg_dict)
    # oscilloscope y-scale
    voltage = afg.param['Fvoltage']
    ch2 = afg.param['ch2']
    freq = afg.param['freq']
    flabel = float2expo(freq)
    for ch_mdo, chan_uub in ((cho, 1), (ch9, ch9ampli)):
        scale = 2 * voltage * splitter_amplification(ch2, chan_uub) / 1.0
        mdo.send('CH%d:SCALE %f' % (ch_mdo, scale))
    mdo.send('ACQUIRE:STATE ON')
    sleep(TOUT_PREP)
    afg.trigger()
    logger.debug('trigger sent')
    sleep(TOUT_DAQ)
    vlabel = item2label(functype='F', ch2=ch2, voltage=voltage, freq=freq)
    label = item2label(functype='F', ch2=ch2, freq=freq)
    if label not in calibdata:
        calibdata[label] = []
    yall = np.zeros((N, 0))
    for ch_mdo, typ in ((cho, 'datao'), (ch9, 'data9')):
        res = mdo.readWFM(ch_mdo, dataslice)
        fname = datadir + typ + '_' + vlabel + '.txt'
        np.savetxt(fname, res[0])
        yall = np.append(yall, res[0].reshape((N, 1)), axis=1)
    resd = sf.fit(yall, flabel, freq, SineFitter.CHI)
    calibdata[label].append((voltage, resd['ampli'].tolist()))
    datalog.write(dlog_formstr % tuple([flabel, freq/1e6, ch2, voltage] +
                                       resd['ampli'].tolist() +
                                       resd['chi'].tolist() +
                                       resd['param'].flatten('C').tolist()))
afg.switchOn(False)
datalog.close()

# calculate sensitivity fit calibdata vs. voltage
calibration = {}
for label in sorted(calibdata.keys(),
                    key=lambda label: (label2item(label)['ch2'],
                                       label2item(label)['freq'])):
    vallist = calibdata[label]
    item = label2item(label)
    sampo = splitter_amplification(item['ch2'], 1)
    samp9 = splitter_amplification(item['ch2'], ch9ampli)
    # [[ v_i, ampo_i, amp9_i], ...]
    xy = np.zeros((len(vallist), 3))
    for i, (voltage, [ampo, amp9]) in enumerate(vallist):
        xy[i, :] = voltage, ampo/sampo, amp9/samp9
    # xx_xy = [v1, v2, ...] * xy
    xx_xy = xy.T[0].dot(xy)
    slopes = xx_xy[1:] / xx_xy[0]
    # correlation coeff = cov(V, ADC) / sqrt(var(V) * var(ADC))
    if xy.shape[0] > 1:
        covm = np.cov(xy, rowvar=False)
        coeffo = 1.0 - covm[0][1] / np.sqrt(covm[0][0] * covm[1][1])
        coeff9 = 1.0 - covm[0][2] / np.sqrt(covm[0][0] * covm[2][2])
    else:
        coeffo, coeff9 = 0.0, 0.0
    freq = item['freq']
    flabel = float2expo(freq)
    fitlog.write(fit_formstr % (flabel, freq/1.e6, item['ch2'],
                                slopes[0], slopes[1], coeffo, coeff9))
    calibration[label] = slopes.tolist()

fitlog.close()

with open(datadir + 'calib.json', 'w') as fp:
    json.dump(calibration, fp, indent=2, sort_keys=True)
