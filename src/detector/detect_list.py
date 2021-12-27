from src.detector.detectors.g4000.detector4012 import Detector4012

DETECT_EVENT_CODES = {
    ## 동작 이상
    4012: {'module': 'combinerBox', 'func': Detector4012},
}

def DETECT_LIST(sharedData):
    detectList = {
        'inverter': [],
        'pcs': [],
        'bms': [],
        'pms': [],
        'meter': [],
        'emsEnv': [],
        'bmsRack': [],
        'combinerBox': [],
        'weather': [],
        'machineIndex': [],
        'isoMeter': []
    }

    for each in list(DETECT_EVENT_CODES.values()):
        if each.get('func') is not None:
            detectList[each.get('module')].append(each.get('func')(sharedData))

    return detectList
