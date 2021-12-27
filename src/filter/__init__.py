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
            return 'true'
        except Exception:
            return traceback.format_exc()

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
