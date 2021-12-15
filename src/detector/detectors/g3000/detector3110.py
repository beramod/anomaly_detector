import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## ISO METER 내부 통신에러
class Detector3110(BaseDetector30xx):
    _eventCode = 3110

    def _process(self):
        event = self._internalCommError('isoMeter', 'isoMeters', 'isometerno', '')
        return event
