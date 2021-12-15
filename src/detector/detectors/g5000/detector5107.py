from src.detector.detectors.g5000 import Detector5xxx


# Meter Error
class Detector5107(Detector5xxx):
    _eventCode = 5107

    def _process(self):
        data = self._data
        res = self._parseFaults(data, 'meters', 'meterno', 'error')

        if not res.get('detect'):
            return False

        event = self._creatEvent('meter', res.get('modulenos'), {
            'moduleEvents': res.get('moduleEvents'),
            'message': res.get('message'),
            'alarmIds': self._getAlarmIds(res.get('moduleEvents'))
        })

        return event
