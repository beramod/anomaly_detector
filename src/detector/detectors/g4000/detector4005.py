import datetime
from src.detector.detectors import BaseDetector


## BMS 전압 이상
class Detector4005(BaseDetector):
    _eventCode = 4005

    def _process(self):
        data = self._data

        bms = self._sharedData.get('bms', self._mcno)
        if not bms:
            return False

        ## TODO: 재정의 필요. bms가 여러개인데...?
        bmsnos = [1]

        systemV = data.get('bms')[0].get('systemV')

        # event = self._creatEvent('meter', meternos, {})

        return False
