import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## MachineIndex 외부 통신에러
class Detector3008(BaseDetector30xx):
    _eventCode = 3008

    def _process(self):
        event = self._detectCommError('machineIndex', '')
        return event
