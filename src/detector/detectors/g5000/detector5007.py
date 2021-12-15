from src.detector.detectors.g5000 import Detector5xxx


# Meter Warning
class Detector5007(Detector5xxx):
    _eventCode = 5007

    def _process(self):
        data = self._data
        res = self._parseFaults(data, 'meters', 'meterno', 'warning')

        if not res.get('detect'):
            return False

        event = self._creatEvent('meter', res.get('modulenos'), {
            'moduleEvents': res.get('moduleEvents'),
            'message': res.get('message'),
            'alarmIds': self._getAlarmIds(res.get('moduleEvents'))
        })

        return event
