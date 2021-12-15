import datetime
from src.detector.detectors.g5000 import Detector5xxx


## Pms Warning
class Detector5004(Detector5xxx):
    _eventCode = 5004

    def _process(self):
        data = self._data
        res = self._parseFaults(data, 'pms', 'pmsno', 'warning')
        if not res.get('detect'):
            return False

        event = self._creatEvent('pms', res.get('modulenos'), {
            'moduleEvents': res.get('moduleEvents'),
            'message': res.get('message'),
            'alarmIds': self._getAlarmIds(res.get('moduleEvents'))
        })
        return event
