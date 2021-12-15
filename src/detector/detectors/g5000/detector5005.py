import datetime
from src.detector.detectors.g5000 import Detector5xxx


## IsoMeter Warning
class Detector5005(Detector5xxx):
    _eventCode = 5005

    def _process(self):
        data = self._data
        res = self._parseFaults(data, 'isoMeters', 'isometerno', 'warning')

        if not res.get('detect'):
            return False

        event = self._creatEvent('isoMeter', res.get('modulenos'), {
            'moduleEvents': res.get('moduleEvents'),
            'message': res.get('message'),
            'alarmIds': self._getAlarmIds(res.get('moduleEvents'))
        })

        return event
