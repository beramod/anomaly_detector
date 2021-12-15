import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## Weather 내부 통신에러
class Detector3107(BaseDetector30xx):
    _eventCode = 3107

    def _process(self): # wind
        event = self._internalCommError('weather', 'insolations', 'insolationno', '')
        return event