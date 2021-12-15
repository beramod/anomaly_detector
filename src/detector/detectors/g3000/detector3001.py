import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## Inverter 외부 통신에러
class Detector3001(BaseDetector30xx):
    _eventCode = 3001

    def _process(self):
        event = self._detectModuleCommError('inverter', 'inverters', 'invno', '')
        return event
