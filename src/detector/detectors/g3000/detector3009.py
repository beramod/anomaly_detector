import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## CombinerBox 외부 통신에러
class Detector3009(BaseDetector30xx):
    _eventCode = 3009

    def _process(self):
        event = self._detectModuleCommError('combinerBox', 'combinerBoxes', 'combinerboxno', '')
        return event
