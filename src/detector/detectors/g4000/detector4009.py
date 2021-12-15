import datetime
from src.detector.detectors.g4000 import BaseDetectorForTemp


## CELL TEMP Warning
## 추후 개발 필요
class Detector4009(BaseDetectorForTemp):
    _eventCode = 4009

    def _process(self):
        pass