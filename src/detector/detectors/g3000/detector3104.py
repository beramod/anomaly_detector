import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## METER 내부 통신에러
class Detector3104(BaseDetector30xx):
    _eventCode = 3104

    def _process(self):
        event = self._internalCommError('meter', 'meters', 'meterno', '')
        return event
