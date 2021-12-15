import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## PCS 내부 통신에러
class Detector3102(BaseDetector30xx):
    _eventCode = 3102

    def _process(self):
        event = self._internalCommError('pcs', 'pcs', 'pcsno', '')
        return event
