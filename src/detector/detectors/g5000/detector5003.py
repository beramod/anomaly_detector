import datetime
from src.detector.detectors.g5000 import Detector5xxx


## Inverter Warning
class Detector5003(Detector5xxx):
    _eventCode = 5003

    def _process(self):
        data = self._data

        res = self._parseFaults(data, 'inverters', 'invno', 'warning')

        if not res.get('detect'):
            return False

        event = self._creatEvent('inverter', res.get('modulenos'), {
            'moduleEvents': res.get('moduleEvents'),
            'message': res.get('message'),
            'alarmIds': self._getAlarmIds(res.get('moduleEvents'))
        })
        return event
