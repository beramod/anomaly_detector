import datetime
from src.detector.detectors.g5000 import Detector5xxx


## PCS Warning
class Detector5002(Detector5xxx):
    _eventCode = 5002

    def _process(self):
        data = self._data

        res = self._parseFaults(data, 'pcs', 'pcsno', 'warning')

        if not res.get('detect'):
            return False

        event = self._creatEvent('pcs', res.get('modulenos'), {
            'moduleEvents': res.get('moduleEvents'),
            'message': res.get('message'),
            'alarmIds': self._getAlarmIds(res.get('moduleEvents'))
        })
        return event
