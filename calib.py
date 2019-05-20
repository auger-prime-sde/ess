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
from afg import AFG
from BME import TrigDelay, PowerControl
from mdo import MDO
from UUB import gener_funcparams
from dataproc import item2label, SplitterGain
from hsfitter import HalfSineFitter, SineFitter

VERSION = "20190516"

try:
    with open(sys.argv[1], 'r') as fp:
        d = json.load(fp)
except (IndexError, IOError, ValueError):
    print("Usage: %s <JSON config file>" % sys.argv[0])
    raise

# timeouts
TOUT_PREP = 0.4   # delay between afg setting and trigger in s
TOUT_DAQ = 0.1    # timeout between trigger and oscilloscope readout
MINVOLTSCALE = 0.005  # minimal voltage scale on MDO [V]

dt = datetime.now()
datadir = dt.strftime(d.get('datadir', './'))
if datadir[-1] != os.sep:
    datadir += os.sep
if not os.path.isdir(datadir):
    os.makedirs(datadir)

if 'comment' in d:
    with open(datadir + 'README.txt', 'w') as f:
        f.write(d['comment'] + '\n')

if 'logging' in d:
    kwargs = {key: d['logging'][key]
              for key in ('level', 'format', 'filename')
              if key in d['logging']}
    if 'filename' in kwargs:
        kwargs['filename'] = datetime.now().strftime(kwargs['filename'])
        if kwargs['filename'][0] not in ('.', '/'):
            kwargs['filename'] = datadir + kwargs['filename']
    logging.basicConfig(**kwargs)
logger = logging.getLogger('calib')


td_predefined = d.get('trigdelay', {'P': 0, 'F': 30})
td = TrigDelay(d['ports']['trigdelay'], td_predefined)
pc = PowerControl(d['ports']['powercontrol'], None, None, [None]*10)
afgparams = d.get('afgparams', {})
afg = AFG(tmcid=d['usbtmc']['afg'], **afgparams)
afg_chans = [i for i, gain in enumerate(afg.param['gains'])
             if gain is not None]

mdo = MDO(tmcid=d['usbtmc']['mdo'])
mdochans = d.get('mdochans')
ref = mdochans.index('REF') if 'REF' in mdochans else None
try:
    trig = mdochans.index('TRIG')
    mdochans[trig] = None
except IndexError:
    logger.error('No TRIG in mdochans')
    raise

splitgain = SplitterGain(pregains=afg.param['gains'], mdochans=mdochans)

for item in d['setup_mdo']:
    mdo.send(item.format(TRIG=trig+1))
for CH, splitch in splitgain.mdomap.iteritems():
    for item in d['setup_mdoch']:
        mdo.send(item.format(CH=CH))

generF = [rec[2] for rec in gener_funcparams() if rec[1] == 'F'][0]
generP = [rec[2] for rec in gener_funcparams() if rec[1] == 'P'][0]

dataslice = d.get('dataslice')
dataslice_max = (min([ds[0] for ds in dataslice.itervalues()]),
                 max([ds[1] for ds in dataslice.itervalues()]), 1)
FREQs = {'hr': 2500., 'lr': 125.}
nharm = d.get('nharm', 1)
npoly = d.get('npoly', 1)
hswidth = afg.param['hswidth']
sf = {}
hsf = {}
for resol in ('hr', 'lr'):
    if 'freq_' + resol in dataslice:
        start, stop, step = dataslice['freq_' + resol]
        N = (stop - start)/step
        sf[resol] = SineFitter(N=N, FREQ=FREQs[resol],
                               NHARM=nharm, NPOLY=npoly)
        sf[resol].crop = False
    else:
        sf[resol] = None

    if 'pulse_' + resol in dataslice:
        start, stop, step = dataslice['pulse_' + resol]
        N = (stop - start)/step
        if N & (N-1):   # not a power of 2
            logger.warning('N for pulse_%s (%d) not power of 2', resol, N)
        hsf[resol] = HalfSineFitter(hswidth, N=N, FREQ=FREQs[resol],
                                    zInvert=True)
    else:
        hsf[resol] = None

# data logs
if any(hsf):
    datapulses = open(datadir + 'datapulses.txt', 'a')
    prolog = """\
# calibration of splitter by pulses
# {dt:%Y-%m-%d}
# columns: splitch | resol | splitmode | Pvoltage[V] |
#          ampli[V] | pede[V] | chi[V]
""".format(dt=dt)
    datapulses.write(prolog)
    logdata = ['{splitch:3s}', '{resol:2s}',
               '{splitmode:1d}', '{voltage:3.1f}',
               '{ampli:7.5f}', '{pede:8.5f}', '{chi:7.5f}']
    datapulses_formstr = '  '.join(logdata) + '\n'
    fitpulses = {splitch: [] for splitch in splitgain.mdomap.itervalues()}

if any(sf):
    datafreqs = open(datadir + 'datafreqs.txt', 'a')
    prolog = """\
# calibration of splitter by sines
# {dt:%Y-%m-%d}
# columns: splitch | resol | flabel | freq[MHz] | splitmode | Fvoltage[V] |
#          ampli[V] | chi[V] | <params>
# params: cos/sin coeffs[V]: n*omega, n = 1..{nharm:d}
#         vandermont coeffs: x**k, k = 0..{npoly:d}
#             x = (2*t + 1)/N - 1, t = 0..N-1 - timebin
""".format(dt=dt, nharm=nharm, npoly=npoly)
    datafreqs.write(prolog)
    logdata = ['{splitch:2s}', '{resol:2s}',
               '{flabel:3s}', '{freq:6.2f}', '{splitmode:1d}',
               '{voltage:3.1f}', '{ampli:7.5f}', '{chi:7.5f}']
    datafreqs_formstr = '  '.join(logdata)
    fitfreqs = {splitch: [] for splitch in splitgain.mdomap.itervalues()}


# data acquisition - pulses
if 'P' in d['daqparams'] and any(hsf):
    td.delay = 'P'
    afg.setParams(functype='P')
    afg.switchOn(True, afg_chans)
    for afg_dict, item_dict in generP(**d['daqparams']['P']):
        afg.setParams(**afg_dict)
        if 'splitmode' in item_dict:
            splitmode = item_dict['splitmode']
            pc.splitterMode(splitmode)
        else:
            splitmode = None
        # set optimal MDO vertical scale for each channel
        for mdoch in splitgain.mdomap.iterkeys():
            amplif = splitgain.gainMDO(splitmode, mdoch)
            scale = item_dict['voltage'] * amplif / 6.0
            if scale < MINVOLTSCALE:
                scale = MINVOLTSCALE
            mdo.send('CH%d:SCALE %f' % (mdoch, scale))
        mdo.send('ACQUIRE:STATE ON')
        sleep(TOUT_PREP)
        afg.trigger()
        logger.debug('trigger sent')
        sleep(TOUT_DAQ)
        for mdoch, splitch in splitgain.mdomap.iteritems():
            res = mdo.readWFM(mdoch, dataslice_max)
            fname = datadir + item2label(item_dict, splitch=splitch) + '.txt'
            logger.info('saving %s', fname)
            np.savetxt(fname, res[0])
            for resol in ('hr', 'lr'):
                if not hsf[resol]:
                    continue
                start, stop, step = dataslice['pulse_' + resol]
                N = (stop - start)/step
                yall = res[0][start:stop:step].reshape((N, 1))
                resfit = hsf[resol].fit(yall, HalfSineFitter.CHI)
                ampli = resfit['ampli'][0]
                datapulses.write(datapulses_formstr.format(
                    splitch=splitch, resol=resol, ampli=ampli,
                    pede=resfit['pede'][0], chi=resfit['chi'][0], **item_dict))
                eV = splitgain.gainMDO(splitmode, mdoch)*item_dict['voltage']
                fitpulses[splitch].append((resol, eV, ampli))
    afg.switchOn(False, afg_chans)

# data acquisition - freqs
if 'F' in d['daqparams'] and any(sf):
    td.delay = 'F'
    afg.setParams(functype='F')
    afg.switchOn(True, afg_chans)
    for afg_dict, item_dict in generF(**d['daqparams']['F']):
        afg.setParams(**afg_dict)
        freq, flabel = item_dict['freq'], item_dict['flabel']
        if 'splitmode' in item_dict:
            splitmode = item_dict['splitmode']
            pc.splitterMode(splitmode)
        else:
            splitmode = None
        # set optimal MDO vertical scale for each channel
        for mdoch in splitgain.mdomap.iterkeys():
            amplif = splitgain.gainMDO(splitmode, mdoch)
            scale = 2 * item_dict['voltage'] * amplif / 6.0
            if scale < MINVOLTSCALE:
                scale = MINVOLTSCALE
            mdo.send('CH%d:SCALE %f' % (mdoch, scale))
        mdo.send('ACQUIRE:STATE ON')
        sleep(TOUT_PREP)
        afg.trigger()
        logger.debug('trigger sent')
        sleep(TOUT_DAQ)
        for mdoch, splitch in splitgain.mdomap.iteritems():
            res = mdo.readWFM(mdoch, dataslice_max)
            fname = datadir + item2label(item_dict, splitch=splitch) + '.txt'
            logger.info('saving %s', fname)
            np.savetxt(fname, res[0])
            for resol in ('hr', 'lr'):
                if not hsf[resol]:
                    continue
                start, stop, step = dataslice['freq_' + resol]
                N = (stop - start)/step
                yall = res[0][start:stop:step].reshape((N, 1))
                resfit = sf[resol].fit(yall, flabel, freq, SineFitter.CHI)
                ampli = resfit['ampli'][0]
                datafreqs.write(datafreqs_formstr.format(
                    splitch=splitch, resol=resol,
                    flabel=flabel, freq=1e-6*freq,
                    splitmode=item_dict['splitmode'],
                    voltage=item_dict['voltage'],
                    ampli=ampli, chi=resfit['chi'][0]))
                params = resfit['param'].flatten('C').tolist()
                datafreqs.write('  ' + ' '.join(['%9.6f' % par
                                                 for par in params]))
                datafreqs.write('\n')
                eV = splitgain.gainMDO(splitmode, mdoch)*item_dict['voltage']
                fitfreqs[splitch].append((resol, eV, 'f'+flabel, ampli))
    afg.switchOn(False, afg_chans)

# epilog
if any(hsf):
    datapulses.close()
    with open(datadir + 'fitpulses.json', 'a') as fp:
        json.dump(fitpulses, fp, indent=2)
if any(sf):
    datafreqs.close()
    with open(datadir + 'fitfreqs.json', 'a') as fp:
        json.dump(fitfreqs, fp, indent=2)
