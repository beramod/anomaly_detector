import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## Weather 외부 통신에러
class Detector3007(BaseDetector30xx):
    _eventCode = 3007

    def _process(self):
        event = self._detectCommError('weather', '')
        return event
