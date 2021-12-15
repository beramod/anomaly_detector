from src.detector.detectors.g4000.detector4001 import Detector4001
from src.detector.detectors.g4000.detector4002 import Detector4002
from src.detector.detectors.g4000.detector4003 import Detector4003
from src.detector.detectors.g4000.detector4004 import Detector4004
from src.detector.detectors.g4000.detector4006 import Detector4006
from src.detector.detectors.g4000.detector4007 import Detector4007
from src.detector.detectors.g4000.detector4008 import Detector4008
from src.detector.detectors.g4000.detector4010 import Detector4010
from src.detector.detectors.g4000.detector4011 import Detector4011
from src.detector.detectors.g4000.detector4012 import Detector4012

from src.detector.detectors.g5000.detector5001 import Detector5001
from src.detector.detectors.g5000.detector5002 import Detector5002
from src.detector.detectors.g5000.detector5003 import Detector5003
from src.detector.detectors.g5000.detector5004 import Detector5004
from src.detector.detectors.g5000.detector5005 import Detector5005
from src.detector.detectors.g5000.detector5006 import Detector5006
from src.detector.detectors.g5000.detector5007 import Detector5007
from src.detector.detectors.g5000.detector5101 import Detector5101
from src.detector.detectors.g5000.detector5102 import Detector5102
from src.detector.detectors.g5000.detector5103 import Detector5103
from src.detector.detectors.g5000.detector5104 import Detector5104
from src.detector.detectors.g5000.detector5105 import Detector5105
from src.detector.detectors.g5000.detector5106 import Detector5106
from src.detector.detectors.g5000.detector5107 import Detector5107

# func가 None 인것은 anomaly detector에서 다루는 이벤트 이지만 detector에서 처리하지는 않는 event

DETECT_EVENT_CODES = {
    ## 외부 통신 에러
    3001: {'module': 'inverter', 'func': None},
    3002: {'module': 'pcs', 'func': None},
    3003: {'module': 'bms', 'func': None},
    3004: {'module': 'meter', 'func': None},
    3005: {'module': 'pms', 'func': None},
    3006: {'module': 'emsEnv', 'func': None},
    3007: {'module': 'weather', 'func': None},
    3008: {'module': 'machineIndex', 'func': None},
    3009: {'module': 'combinerBox', 'func': None},
    3011: {'module': 'isoMeter', 'func': None},

    ## 내부 통신 에러
    3101: {'module': 'inverter', 'func': None},
    3102: {'module': 'pcs', 'func': None},
    3103: {'module': 'bms', 'func': None},
    3104: {'module': 'meter', 'func': None},
    3105: {'module': 'pms', 'func': None},
    # 3106: {'module': 'emsEnv', 'func': None},
    3107: {'module': 'weather', 'func': None},
    3109: {'module': 'combinerBox', 'func': None},
    3110: {'module': 'isoMeter', 'func': None},

    ## 동작 이상
    4001: {'module': 'inverter', 'func': Detector4001},
    4002: {'module': 'pcs', 'func': Detector4002},
    4003: {'module': 'pcs', 'func': Detector4003},
    4006: {'module': 'emsEnv', 'func': Detector4006},
    4007: {'module': 'emsEnv', 'func': Detector4007},
    4008: {'module': 'bms', 'func': Detector4008},
    4010: {'module': 'emsEnv', 'func': Detector4010},
    4011: {'module': 'emsEnv', 'func': Detector4011},
    4012: {'module': 'combinerBox', 'func': Detector4012},

    ## 모듈 warning&fault
    5001: {'module': 'bms', 'func': Detector5001},
    5002: {'module': 'pcs', 'func': Detector5002},
    5003: {'module': 'inverter', 'func': Detector5003},
    5004: {'module': 'pms', 'func': Detector5004},
    5005: {'module': 'isoMeter', 'func': Detector5005},
    5006: {'module': 'combinerBox', 'func': Detector5006},
    5007: {'module': 'meter', 'func': Detector5007},
    5101: {'module': 'bms', 'func': Detector5101},
    5102: {'module': 'pcs', 'func': Detector5102},
    5103: {'module': 'inverter', 'func': Detector5103},
    5104: {'module': 'pms', 'func': Detector5104},
    5105: {'module': 'isoMeter', 'func': Detector5105},
    5106: {'module': 'combinerBox', 'func': Detector5106},
    5107: {'module': 'meter', 'func': Detector5107}
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
