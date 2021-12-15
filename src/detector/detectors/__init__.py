import datetime, uuid, math
import traceback

from src.manager.shared_data import SharedData
from src.util.nines_request import NinesRequests


class BaseDetector:
    _eventCode = 0
    _detectPsStatusCodes = [2, 3, 8]

    def __init__(self, sharedData):
        self._sharedData = sharedData
        self._eventSpec = self._sharedData.get('eventSpec', self._eventCode)
        self._eventAlarmSpec = self._sharedData.get('eventAlarmSpec', self._eventCode)
        self._mcno = None
        self._data = None
        self._powerStationInfo = None
        self._now = None
        self._timestamp = None
        self._ninesApi = NinesRequests()

    def getEventCode(self):
        return self._eventCode

    def detect(self, data, timestamp):
        try:
            self._data = data
            self._timestamp = timestamp
            if not self._preProcess():
                return False
            event = self._process()
            self._postProcess()
            return event
        except Exception as e:
            self._init()
            return False

    def _preProcess(self):
        self._mcno = self._data.get('mcno')
        detectTime = self._eventSpec.get('detectTime')
        if self._mcno > 99999:
            self._powerStationInfo = self._sharedData.get('virtualPs', self._mcno)
        else:
            self._powerStationInfo = self._sharedData.get('powerStation', self._mcno)

        if not self._powerStationInfo:
            return False

        if self._powerStationInfo.get('psStatusCode') not in self._detectPsStatusCodes:
            return False

        nowTime = int(self._timestamp.strftime('%H%M'))

        if not self._powerStationInfo:
            return False
        if not self._checkDetectTime(detectTime, nowTime):
            return False
        return True

    ## override
    def _process(self):
        return False

    def _postProcess(self):
        self._init()

    def _init(self):
        self._mcno = None
        self._data = None
        self._now = None
        self._powerStationInfo = None
        self._timestamp = None

    def _creatEvent(self, module, moduleNos, data):
        pendingClearTime = None
        state = 'event'
        pendingTime = self._eventSpec.get('pendingTime')
        alertTime = self._eventSpec.get('alertTime')
        nowTime = int(self._timestamp.strftime('%H%M'))

        if pendingTime != 0:
            pendingClearTime = self._timestamp + datetime.timedelta(seconds=pendingTime)
            state = 'pending'

        # if not self._checkAlertTime(alertTime, nowTime):
        #     state = 'sleep'

        message = ''

        if moduleNos:
            message += '모듈번호: {}\n'.format(str(moduleNos).replace('[', '').replace(']', ''))
                # + '{}'.format(data.get('message'))
        if data.get('message'):
            message += data.get('message')
        if data.get('alarmIds') is None:
            data['alarmIds'] = []
        if self._eventAlarmSpec is not None:
            data['alarmIds'].append(self._eventAlarmSpec.get('alarmId'))
            data['alarmIds'] = list(set(data.get('alarmIds')))

        data['option'] = self._eventSpec.get('option')

        return {
            'eventId': self._timestamp.strftime('%Y%m%d%HH%MM%SS') + str(uuid.uuid4()),
            'eventCode': self._eventSpec.get('eventCode'),
            'eventName': self._eventSpec.get('eventName'),
            'mcno': self._powerStationInfo.get('mcno'),
            'psName': self._powerStationInfo.get('psName'),
            'module': module,
            'moduleNos': moduleNos,
            'startTime': self._timestamp,
            'endTime': None,
            'state': state,
            'isSend': False,
            'pendingClearTime': pendingClearTime,
            'isSoulOnM': self._powerStationInfo.get('manage').get('onm'),
            'message': message,
            'data': data
        }

    def _checkDetectTime(self, detectTime, nowTime):
        if detectTime.get('start') == '0000' and detectTime.get('end') == '0000':
            return True
        if int(detectTime.get('start')) <= nowTime <= int(detectTime.get('end')):
            return True
        return False
    
    def _checkAlertTime(self, alertTime, nowTime):
        if alertTime.get('start') == '0000' and alertTime.get('end') == '0000':
            return True
        if int(alertTime.get('start')) <= int(nowTime) <= int(alertTime.get('end')):
            return True
        return False

    def _datetimeAtToDatetime(self, datetimeAt):
        yy = int('20' + datetimeAt[:2])
        mm = int(datetimeAt[2:4])
        dd = int(datetimeAt[4:6])
        HH = int(datetimeAt[6:8])
        MM = int(datetimeAt[8:10])
        SS = int(datetimeAt[10:12])
        return datetime.datetime(yy, mm, dd, HH, MM, SS)
    
    def _updateTimeAtToDatetime(self, updateTime):
        updateTime = updateTime.replace('-', '')
        updateTime = updateTime.replace(' ', '')
        updateTime = updateTime.replace(':', '')
        return self._datetimeAtToDatetime(updateTime[2:])

    def _checkCollectTimeDiff(self, datetimeAt):
        now = datetime.datetime.now()
        target = ''

        if '-' in datetimeAt:
            target = self._updateTimeAtToDatetime(datetimeAt)
        else:
            target = self._datetimeAtToDatetime(datetimeAt)

        return (now - target).total_seconds()
