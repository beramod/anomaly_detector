import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## PCS 외부 통신에러
class Detector3002(BaseDetector30xx):
    _eventCode = 3002

    def _process(self):
        event = self._detectModuleCommError('pcs', 'pcs', 'pcsno', '')
        return event
