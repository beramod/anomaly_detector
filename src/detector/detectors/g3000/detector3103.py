import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## BMS 내부 통신에러
class Detector3103(BaseDetector30xx):
    _eventCode = 3103

    def _process(self):
        event = self._internalCommError('bms', 'bms', 'bmsno', '')
        return event
