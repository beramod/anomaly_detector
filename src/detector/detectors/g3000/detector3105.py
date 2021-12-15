import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## PMS 내부 통신에러
class Detector3105(BaseDetector30xx):
    _eventCode = 3105

    def _process(self):
        event = self._internalCommError('pms', 'pms', 'pmsno', '')
        return event
