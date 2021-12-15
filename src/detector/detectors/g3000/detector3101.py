import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## Inverter 내부 통신에러
class Detector3101(BaseDetector30xx):
    _eventCode = 3101

    def _process(self):
        event = self._internalCommError('inverter', 'inverters', 'invno', '')
        return event
