import datetime
from src.detector.detectors.g3000 import BaseDetector30xx

## ISO METER 외부 통신에러
class Detector3011(BaseDetector30xx):
    _eventCode = 3011

    def _process(self):
        event = self._detectModuleCommError('isoMeter', 'isoMeters', 'isometerno', '')
        return event
