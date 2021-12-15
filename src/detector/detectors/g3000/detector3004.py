import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## METER 외부 통신에러
class Detector3004(BaseDetector30xx):
    _eventCode = 3004

    def _process(self):
        event = self._detectModuleCommError('meter', 'meters', 'meterno', '')
        return event
