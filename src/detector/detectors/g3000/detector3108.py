import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## MachineIndex 외부 통신에러
class Detector3108(BaseDetector30xx):
    _eventCode = 3108

    def _process(self):
        event = self._internalCommError('machineIndex', '')
        return event
