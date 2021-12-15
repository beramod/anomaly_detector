import datetime
from src.detector.detectors import BaseDetector


## Inverter 검증
class Detector4001(BaseDetector):
    _eventCode = 4001

    def _process(self):
        data = self._data

        invnos = []

        for each in data.get('inverters'):
            invno = each.get('invno')
            ackw = each.get('ackw')
            if ackw <= 0:
                invnos.append(invno)

        if len(invnos) == 0:
            return False

        event = self._creatEvent('inverter', invnos, {})

        return event
