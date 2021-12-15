import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## BMS 외부 통신에러
class Detector3003(BaseDetector30xx):
    _eventCode = 3003

    def _process(self):
        event = self._detectModuleCommError('bms', 'bms', 'bmsno', '')
        return event
