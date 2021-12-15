from src.detector.detectors.g5000 import Detector5xxx


# 접속반 Error
class Detector5106(Detector5xxx):
    _eventCode = 5106

    def _process(self):
        data = self._data
        res = self._parseFaults(data, 'combinerBoxes', 'combinerboxno', 'error')

        if not res.get('detect'):
            return False

        event = self._creatEvent('combinerBox', res.get('modulenos'), {
            'moduleEvents': res.get('moduleEvents'),
            'message': res.get('message'),
            'alarmIds': self._getAlarmIds(res.get('moduleEvents'))
        })

        return event
