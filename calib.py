"""
  ESS procedure
  Calibrate AFG/cabels/splitter path
"""

import sys
import os
import json
import logging
import shutil
from datetime import datetime
from time import sleep
import numpy as np

# ESS stuff
from afg import AFG
from BME import PowerControl
from mdo import MDO
from UUB import gener_funcparams
from dataproc import item2label, SplitterGain
from hsfitter import HalfSineFitter, SineFitter

VERSION = "20200505"

# timeouts & constants
TOUT_PREP = 0.4   # delay between afg setting and trigger in s
TOUT_DAQ = 0.1    # timeout between trigger and oscilloscope readout
MINVOLTSCALE = 0.0005  # minimal voltage scale on MDO [V]


def mdoSetVert(splitmode, volt_pp, mdo, splitgain, offsets, logger):
    """Set optimal MDO vertical scale and offset for each channel"""
    NDIV = 6.0  # number of divisions for volt_pp
    lsb = []
    for mdoch, splitch in splitgain.mdomap.items():
        amplif = splitgain.gainMDO(splitmode, mdoch)
        scale = volt_pp * amplif / NDIV
        offset, noise = offsets[(splitch, splitmode)]
        offset -= scale * NDIV/2
        mdo.send('CH%d:OFFSET %f' % (mdoch, offset))
        scale = max(scale, MINVOLTSCALE, 5*noise)
        mdo.send('CH%d:SCALE %f' % (mdoch, scale))
        # read back scale, there is 25 dig. levels per div on 1B
        resp = mdo.send('CH%d:SCALE?' % mdoch, resplen=20)
        scale = float(resp)
        lsb.append('%s: %f' % (splitch, scale/25 * 1000))
    logger.info('LSBs set by SCALE [mV]: %s', ', '.join(lsb))


def calibOffsets(mdo, afg, pc, mdomap, fp, formstr, logger):
    """Find offset value and noise, write results to file
return dict {splitch: offset value}"""
    INITSCALE = 0.04  # offset must be in <-5*INITSCALE, 5*INITSCALE>, [volt]
    offsets = {(splitch, splitmode): (None, None)
               for splitch in mdomap.values() for splitmode in (0, 1, 3)}
    logger.info('Offset calibration, coarse estimation LSB: %f mV',
                INITSCALE/25 * 1000)
    for splitmode in (0, 1, 3):
        pc.splitterMode = splitmode
        for mdoch in mdomap.keys():
            mdo.send('CH%d:OFFSET 0.0' % mdoch)
            mdo.send('CH%d:SCALE %f' % (mdoch, INITSCALE))
        sleep(TOUT_PREP)
        trigger()
        sleep(TOUT_DAQ)
        # readout coarse offset + noise and set
        for mdoch, splitch in mdomap.items():
            res = mdo.readWFM(mdoch)
            offset = np.mean(res[0])
            noise = np.std(res[0])
            logger.debug('%s:%d coarse read offset: %.3f mV, noise %.3f mV',
                         splitch, splitmode, 1000*offset, 1000*noise)
            mdo.send('CH%d:OFFSET %f' % (mdoch, offset))
            resp = mdo.send('CH%d:OFFSET?' % mdoch, resplen=20)
            offset = float(resp)
            scale = max(MINVOLTSCALE, noise)
            mdo.send('CH%d:SCALE %f' % (mdoch, scale))
            resp = mdo.send('CH%d:SCALE?' % mdoch, resplen=20)
            scale = float(resp)
            logger.debug(
                '%s:%d fine set: offset %.5f V, scale %.5f V, LSB %.2f mV',
                splitch, splitmode, offset, scale, scale / 25 * 1000)
        # fine readout
        sleep(TOUT_PREP)
        trigger()
        sleep(TOUT_DAQ)
        for mdoch, splitch in mdomap.items():
            res = mdo.readWFM(mdoch)
            offset = np.mean(res[0])
            noise = np.std(res[0])
            offsets[(splitch, splitmode)] = offset, noise
            logger.debug('%s:%d fine read offset: %.3f mV, noise %.3f mV',
                         splitch, splitmode, 1000*offset, 1000*noise)
            fp.write(formstr.format(splitch=splitch, splitmode=splitmode,
                                    offset=1000*offset, noise=1000*noise))
    return offsets


try:
    jsfn = sys.argv[1]
    with open(jsfn, 'r') as fp:
        d = json.load(fp)
    # replace MDO channels from command line
    _mdochans = sys.argv[2:]
    _nch = len(_mdochans)
    assert _nch <= 4
    if _nch > 0:
        d['mdochans'][:_nch] = _mdochans
    assert any([ch is not None for ch in d['mdochans']]), "No MDO channel"
except (IndexError, IOError, ValueError, AssertionError):
    print("Usage: %s <JSON config file> [<MDO channels>]" % sys.argv[0])
    raise

dt = datetime.now()
datadir = dt.strftime(d.get('datadir', './'))
datadir = datadir.replace('$CHS', ''.join(d['mdochans']))
if datadir[-1] != os.sep:
    datadir += os.sep
if not os.path.isdir(datadir):
    os.makedirs(datadir)
shutil.copy(jsfn, datadir)

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


# triggering: either TrigDelay or AFG (with dataslice adjusted)
# td_predefined = d.get('trigdelay', {'P': 0, 'F': 30})
# td = TrigDelay(d['ports']['trigdelay'], td_predefined)
# trigger = TrigDelay.trigger
pc = PowerControl(d['ports']['powercontrol'])
afgparams = d.get('afgparams', {})
afg = AFG(d['ports']['afg'], **afgparams)
trigger = afg.trigger
afg_chans = [i for i, gain in enumerate(afg.param['gains'])
             if gain is not None]

mdo = MDO(d['ports']['mdo'])
mdochans = d.get('mdochans')
if 'TRIG' in mdochans:
    trig = mdochans.index('TRIG')
    mdochans[trig] = None
else:
    trig = -1  # placeholder for item.format

splitgain = SplitterGain(pregains=afg.param['gains'], mdochans=mdochans)

for item in d['setup_mdo']:
    mdo.send(item.format(TRIG=trig+1))
for CH, splitch in splitgain.mdomap.items():
    for item in d['setup_mdoch']:
        mdo.send(item.format(CH=CH))
if 'mdo_delays' in d:
    mdo_delays = {}
    for functype in 'P', 'F':
        mdo_delays[functype] = d['mdo_delays'].get(functype, 20.0)
else:
    mdo_delays = {'P': 20.0, 'F': 20.0}

generF = [rec[2] for rec in gener_funcparams() if rec[1] == 'F'][0]
generP = [rec[2] for rec in gener_funcparams() if rec[1] == 'P'][0]

dataslice = d.get('dataslice')
dataslice_max = (min([ds[0] for ds in dataslice.values()]),
                 max([ds[1] for ds in dataslice.values()]), 1)
mdo.send('DATA:START %d; STOP %d' % (dataslice_max[0]+1, dataslice_max[1]))
FREQs = {'hr': 2500., 'lr': 125.}
nharm = d.get('nharm', 1)
npoly = d.get('npoly', 1)
hswidth = afg.param['hswidth']
sf = {}
hsf = {}
for resol in ('hr', 'lr'):
    if 'freq_' + resol in dataslice:
        start, stop, step = dataslice['freq_' + resol]
        N = (stop - start) // step
        sf[resol] = SineFitter(N=N, FREQ=FREQs[resol],
                               NHARM=nharm, NPOLY=npoly)
        sf[resol].crop = False
    else:
        sf[resol] = None

    if 'pulse_' + resol in dataslice:
        start, stop, step = dataslice['pulse_' + resol]
        N = (stop - start) // step
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
# columns: splitch | resol | splitmode | Pvoltage[V] | index |
#          ampli[V] | pede[V] | chi[V]
""".format(dt=dt)
    datapulses.write(prolog)
    logdata = ['{splitch:3s}', '{resol:2s}',
               '{splitmode:1d}', '{voltage:3.1f}', '{index:03d}',
               '{ampli:7.5f}', '{pede:8.5f}', '{chi:7.5f}']
    datapulses_formstr = '  '.join(logdata) + '\n'
    fitpulses = {splitch: [] for splitch in splitgain.mdomap.values()}

if any(sf):
    datafreqs = open(datadir + 'datafreqs.txt', 'a')
    prolog = """\
# calibration of splitter by sines
# {dt:%Y-%m-%d}
# columns: splitch | resol | flabel | freq[MHz] |
#          splitmode | Fvoltage[V] | index |
#          ampli[V] | chi[V] | <params>
# params: cos/sin coeffs[V]: n*omega, n = 1..{nharm:d}
#         vandermont coeffs: x**k, k = 0..{npoly:d}
#             x = (2*t + 1)/N - 1, t = 0..N-1 - timebin
""".format(dt=dt, nharm=nharm, npoly=npoly)
    datafreqs.write(prolog)
    logdata = ['{splitch:2s}', '{resol:2s}',
               '{flabel:3s}', '{freqm:6.2f}', '{splitmode:1d}',
               '{voltage:3.1f}', '{index:03d}',
               '{ampli:7.5f}', '{chi:7.5f}']
    datafreqs_formstr = '  '.join(logdata)
    fitfreqs = {splitch: [] for splitch in splitgain.mdomap.values()}

foff = open(datadir + 'offsets.txt', 'a')
prolog = """\
# offsets + noise with no AFG input
# {dt:%Y-%m-%d}
# columns: splitch | splitmode | offset [mV] | noise [mV]
""".format(dt=dt)
foff.write(prolog)
foff_formstr = "{splitch:2s} {splitmode:1d}  {offset:7.3f}  {noise:5.3f}\n"

print('Calibrating offsets', file=sys.stderr)
mdo.send('ACQUIRE:STATE ON')
afg.switchOn(False, afg_chans)
offsets = calibOffsets(mdo, afg, pc, splitgain.mdomap,
                       foff, foff_formstr, logger)
foff.close()

# data acquisition - pulses
if 'P' in d['daqparams'] and any(hsf):
    # td.delay = 'P'
    logger.info('Pulse measurement')
    print('Pulse measurement', file=sys.stderr)
    mdo.send('HORIZONTAL:POSITION %f' % mdo_delays['P'])
    afg.setParams(functype='P')
    afg.switchOn(True, afg_chans)
    for afg_dict, item_dict in generP(**d['daqparams']['P']):
        if afg_dict is not None:
            msg = '  splitmode = %d, voltage = %.1fV' % (
                item_dict.get('splitmode', -1),
                item_dict.get('voltage', -1))
            logger.info(msg)
            print(msg, file=sys.stderr)
            afg.setParams(**afg_dict)
            if 'splitmode' in item_dict:
                splitmode = item_dict['splitmode']
                pc.splitterMode = splitmode
            else:
                splitmode = None
            mdoSetVert(pc.splitterMode, item_dict['voltage'], mdo, splitgain,
                       offsets, logger)
            sleep(TOUT_PREP)
        trigger()
        logger.debug('trigger sent')
        sleep(TOUT_DAQ)
        for mdoch, splitch in splitgain.mdomap.items():
            fname = datadir + item2label(item_dict, splitch=splitch) + '.txt'
            res = mdo.readWFM(mdoch, fn=fname)
            # logger.info('saving %s', fname+'.txt')
            # np.savetxt(fname, res[0], fmt='%8.5f')
            # logger.debug('saved')
            for resol in ('hr', 'lr'):
                if not hsf[resol]:
                    continue
                start, stop, step = dataslice['pulse_' + resol]
                N = (stop - start) // step
                yall = res[0][start:stop:step].reshape((N, 1))
                resfit = hsf[resol].fit(yall, HalfSineFitter.CHI)
                ampli = resfit['ampli'][0]
                datapulses.write(datapulses_formstr.format(
                    splitch=splitch, resol=resol, ampli=ampli,
                    pede=resfit['pede'][0], chi=resfit['chi'][0], **item_dict))
                eV = splitgain.gainMDO(splitmode, mdoch)*item_dict['voltage']
                fitpulses[splitch].append((resol, splitmode, eV, ampli))
    afg.switchOn(False, afg_chans)
datapulses.close()

# data acquisition - freqs
if 'F' in d['daqparams'] and any(sf):
    # td.delay = 'F'
    logger.info('Frequency measurement')
    print('Frequency measurement', file=sys.stderr)
    mdo.send('HORIZONTAL:POSITION %f' % mdo_delays['F'])
    afg.setParams(functype='F')
    afg.switchOn(True, afg_chans)
    for afg_dict, item_dict in generF(**d['daqparams']['F']):
        if afg_dict is not None:
            msg = '  splitmode = %d, voltage = %.1fV, freq = %6.2fMHz' % (
                item_dict.get('splitmode', -1),
                item_dict.get('voltage', -1),
                1e-6*item_dict.get('freq', -1e6))
            logger.info(msg)
            print(msg, file=sys.stderr)
            afg.setParams(**afg_dict)
            freq, flabel = item_dict['freq'], item_dict['flabel']
            if afg_dict is not None:
                if 'splitmode' in item_dict:
                    splitmode = item_dict['splitmode']
                    pc.splitterMode = splitmode
                else:
                    splitmode = None
                mdoSetVert(pc.splitterMode, 2*item_dict['voltage'], mdo,
                           splitgain, offsets, logger)
                sleep(TOUT_PREP)
        trigger()
        logger.debug('trigger sent')
        sleep(TOUT_DAQ)
        for mdoch, splitch in splitgain.mdomap.items():
            fname = datadir + item2label(item_dict, splitch=splitch) + '.txt'
            res = mdo.readWFM(mdoch, fn=fname)
            # logger.info('saving %s', fname+'.txt')
            # np.savetxt(fname, res[0], fmt='%8.5f')
            # logger.debug('saved')
            for resol in ('hr', 'lr'):
                if not hsf[resol]:
                    continue
                start, stop, step = dataslice['freq_' + resol]
                N = (stop - start) // step
                logger.debug('reshape')
                yall = res[0][start:stop:step].reshape((N, 1))
                logger.debug('sine fitter, %s, splitmode %d, %.2fMHz, %.1fV',
                             resol, splitmode, freq/1e6, item_dict['voltage'])
                resfit = sf[resol].fit(yall, flabel, freq, SineFitter.CHI)
                logger.debug('sine fit done')
                ampli = resfit['ampli'][0]
                datafreqs.write(datafreqs_formstr.format(
                    splitch=splitch, resol=resol, freqm=1e-6*freq,
                    ampli=ampli, chi=resfit['chi'][0], **item_dict))
                params = resfit['param'].flatten('C').tolist()
                datafreqs.write('  ' + ' '.join(['%9.6f' % par
                                                 for par in params]))
                datafreqs.write('\n')
                eV = splitgain.gainMDO(splitmode, mdoch)*item_dict['voltage']
                fitfreqs[splitch].append(
                    (resol, splitmode, eV, 'f'+flabel, ampli))
    afg.switchOn(False, afg_chans)
datafreqs.close()

mdo.send('ACQUIRE:STATE OFF')
afg.stop()
mdo.stop()

# epilog
if any(hsf):
    datapulses.close()
    with open(datadir + 'fitpulses.json', 'a') as fp:
        json.dump(fitpulses, fp, indent=2)
if any(sf):
    datafreqs.close()
    with open(datadir + 'fitfreqs.json', 'a') as fp:
        json.dump(fitfreqs, fp, indent=2)
