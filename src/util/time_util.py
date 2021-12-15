from dateutil.parser import parse


class TimeUtil:
    @classmethod
    def eventTimeToStr(cls, event):
        if event.get('startTime'):
            event['startTime'] = str(event.get('startTime'))
        if event.get('endTime'):
            event['endTime'] = str(event.get('endTime'))
        if event.get('pendingClearTime'):
            event['pendingClearTime'] = str(event.get('pendingClearTime'))

    @classmethod
    def eventStrTimeToTime(cls, event):
        if event.get('startTime'):
            event['startTime'] = parse(event.get('startTime'))
        if event.get('endTime'):
            event['endTime'] = parse(event.get('endTime'))
        if event.get('pendingClearTime'):
            event['pendingClearTime'] = parse(event.get('pendingClearTime'))
    
    @classmethod
    def strTimeToTime(cls, strTime):
        return parse(strTime)
    
    @classmethod
    def checkAlertTime(cls, alertTime, nowTime):
        if alertTime.get('start') == '0000' and alertTime.get('end') == '0000':
            return True
        if int(alertTime.get('start')) <= int(nowTime) <= int(alertTime.get('end')):
            return True
        return False