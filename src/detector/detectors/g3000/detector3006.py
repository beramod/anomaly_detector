import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## EMS ENV 외부 통신에러
class Detector3006(BaseDetector30xx):
    _eventCode = 3006

    def _process(self):
        event = self._detectCommError('emsEnv', '')
        return event
