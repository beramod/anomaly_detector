import datetime
from src.detector.detectors.g5000 import Detector5xxx


## Inverter Error
class Detector5103(Detector5xxx):
    _eventCode = 5103

    def _process(self):
        data = self._data

        res = self._parseFaults(data, 'inverters', 'invno', 'error')

        if not res.get('detect'):
            return False

        event = self._creatEvent('inverter', res.get('modulenos'), {
            'moduleEvents': res.get('moduleEvents'),
            'message': res.get('message'),
            'alarmIds': self._getAlarmIds(res.get('moduleEvents'))
        })
        
        return event
