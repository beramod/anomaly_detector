import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## PMS 외부 통신에러
class Detector3005(BaseDetector30xx):
    _eventCode = 3005

    def _process(self):
        event = self._detectModuleCommError('pms', 'pms', 'pmsno', '')
        return event
