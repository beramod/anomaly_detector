import datetime
from src.detector.detectors.g5000 import Detector5xxx


## Pms Error
class Detector5104(Detector5xxx):
    _eventCode = 5104

    def _process(self):
        data = self._data

        res = self._parseFaults(data, 'pms', 'pmsno', 'error')

        if not res.get('detect'):
            return False

        event = self._creatEvent('pms', res.get('modulenos'), {
            'moduleEvents': res.get('moduleEvents'),
            'message': res.get('message'),
            'alarmIds': self._getAlarmIds(res.get('moduleEvents'))
        })
        
        return event
