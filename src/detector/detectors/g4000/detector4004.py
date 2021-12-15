import datetime
from src.define import *
from src.detector.detectors import BaseDetector


## 태양광 AC전압 이상 검증(Meter)
class Detector4004(BaseDetector):
    _eventCode = 4004

    def _process(self):
        data = self._data

        meter = self._sharedData.get('meter', self._mcno)
        if not meter:
            return False

        ## TODO: 재정의 필요. meter가 여러개인데...?
        meternos = [1]

        acvr = data.get('meters')[0].get('meterVR')
        acvs = data.get('meters')[0].get('meterVS')
        acvt = data.get('meters')[0].get('meterVT')

        threshold = THRESHOLD_METER_AC_VOLTAGE

        if acvr > threshold and acvs > threshold and acvt > threshold:
            return False

        event = self._creatEvent('meter', meternos, {
            'message': '태양광 AC전압 이상\nACVR: {0:.1f}, ACVS: {1:.1f}, ACVT: {2:.1f}'.format(acvr, acvs, acvt)
        })

        return event
