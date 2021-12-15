import traceback
from src.manager.shared_data import SharedData
from src.detector.detect_list import DETECT_EVENT_CODES
from src.util.system_alerter import SystemAlerter

class Filter:
    def __init__(self) -> None:
        self._detectEventList = list(DETECT_EVENT_CODES.keys())

    def run(self, idx, eventQueue, alertQueue, sharedData):
        clientId = idx
        messageProcErrorCnt = 0
        systemAlerter = SystemAlerter()
        
        try:
            while True:
                try:
                    context = eventQueue.get(clientId)
                    if not context:
                        messageProcErrorCnt = 0
                        continue

                    messageProcErrorCnt = 0

                    self._checkDupEvents(context, sharedData)
                    self._checkCloseEvents(context, sharedData)
                    self._checkSleepEvents(context, sharedData)
                    
                    alertQueue.put(clientId, context)
                except Exception as e:
                    messageProcErrorCnt += 1
                    if messageProcErrorCnt >= 5:
                        systemAlerter.sendAlert('detector(id: {}) message proc failed: {}'.format(
                            clientId, messageProcErrorCnt), traceback.format_exc())
        except Exception as e:
            systemAlerter.sendAlert('detector(id: {}) abort: {}'.format(clientId, messageProcErrorCnt),
                                    traceback.format_exc())

    def process(self, context, sharedData):
        try:
            self._checkDupEvents(context, sharedData)
            self._checkCloseEvents(context, sharedData)
            self._checkSleepEvents(context, sharedData)
            return 'true'
        except Exception:
            return traceback.format_exc()

    def _checkCloseEvents(self, context, sharedData):
        closeEvents = []
        openEvents = []

        for event in context.openEvents:
            key = SharedData.eventKey(event.get('mcno'), event.get('eventCode'))
            existEvent = sharedData.get('event', key)

            if event.get('eventCode') not in self._detectEventList:
                if event.get('endTime'):
                    if existEvent:
                        event['state'] = 'close'
                        closeEvents.append(event)
                    else:
                        event['state'] = 'delete'
                continue
            else:
                if event.get('endTime'):
                    event['state'] = 'close'
                    if not existEvent:
                        event['state'] = 'delete'
                    closeEvents.append(event)
                else:
                    openEvents.append(event)

        context.openEvents = openEvents
        context.closeEvents.extend(closeEvents)

    def _checkDupEvents(self, context, sharedData):
        openEvents = []

        for event in context.openEvents:
            key = SharedData.eventKey(event.get('mcno'), event.get('eventCode'))
            dupEvent = sharedData.get('event', key)
            if dupEvent:
                dupEvent['moduleNos'] = event.get('moduleNos')
                dupEvent['data'].update(event.get('data'))
                dupEvent['message'] = event.get('message')
                dupEvent['modify'] = True
                
                context.events.append(dupEvent)
            else:
                openEvents.append(event)
        context.openEvents = openEvents

    def _checkPendingClearEvents(self, context):
        pendingClearEvents = []

        for event in context.events:
            if event.get('pendingClearTime') and event.get('pendingClearTime') < context.timestamp:
                event['state'] = 'event'
                event['modify'] = True
                pendingClearEvents.append(event)
        
        context.openEvents.extend(pendingClearEvents)
    
    def _checkSleepEvents(self, context, sharedData):
        nowTime = int(context.timestamp.strftime('%H%M'))

        for event in context.openEvents:
            eventCode = event.get('eventCode')
            eventSpec = sharedData.get('eventSpec', eventCode)
            state = event.get('state')
            if state in ['event', 'close']:
                if not self._checkAlertTime(eventSpec.get('alertTime'), nowTime):
                    event['state'] = 'sleep'
                    event['modify'] = True
        
        for event in context.closeEvents:
            eventCode = event.get('eventCode')
            eventSpec = sharedData.get('eventSpec', eventCode)
            state = event.get('state')
            if state in ['event', 'close']:
                if not self._checkAlertTime(eventSpec.get('alertTime'), nowTime):
                    event['state'] = 'sleep'
                    event['modify'] = True

    def _checkDetectTime(self, detectTime, nowTime):
        if detectTime.get('start') == '0000' and detectTime.get('end') == '0000':
            return True
        if int(detectTime.get('start')) <= int(nowTime) <= int(detectTime.get('end')):
            return True
        return False

    def _checkAlertTime(self, alertTime, nowTime):
        if alertTime.get('start') == '0000' and alertTime.get('end') == '0000':
            return True
        if int(alertTime.get('start')) <= int(nowTime) <= int(alertTime.get('end')):
            return True
        return False