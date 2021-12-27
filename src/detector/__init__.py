import time
import traceback
import datetime
from src.util.time_util import TimeUtil
from src.manager.context import Context
from src.util.system_alerter import SystemAlerter
from src.detector.detect_list import DETECT_LIST, DETECT_EVENT_CODES

class Detector:
    _closeEventCheckPeriod = 60
    _pendingEventCheckPeriod = 60
    _sleepEventCheckPeriod = 60

    def run(self, idx, rawQueue, sharedData):
        clientId = idx
        messageProcErrorCnt = 0
        systemAlerter = SystemAlerter()
        detectors = DETECT_LIST(sharedData)

        try:
            while True:
                try:
                    context = rawQueue.get(clientId)

                    if not context:
                        continue

                    result = self.process(context, detectors)
                    if result != 'true':
                        messageProcErrorCnt += 1
                        if messageProcErrorCnt >= 5:
                            systemAlerter.sendAlert('detector(id: {}) message proc failed: {}'.format(
                                clientId, messageProcErrorCnt), traceback.format_exc())
                    else:
                        messageProcErrorCnt = 0
                except:
                    continue
        except Exception as e:
            systemAlerter.sendAlert('detector(id: {}) abort: {}'.format(clientId, messageProcErrorCnt),
                                    traceback.format_exc())

    def process(self, context, detectors):
        try:
            for detector in detectors.get(context.messageType):
                event = None

                try:
                    event = detector.detect(context.raw, context.timestamp)
                except Exception as e:
                    pass
                if not event:
                    continue
                context.openEvents.append(event)

            if len(context.openEvents) > 0:
                return 'true'
            return 'false'
        except Exception:
            return traceback.format_exc()
    
    def closeEventRun(self, eventQueue, sharedData):
        systemAlerter = SystemAlerter()
        codeToDetector = self.getCodeToDetector(sharedData)

        try:
            while True:
                try:
                    events = sharedData.getCategoryObj('event')

                    if not events:
                        continue

                    for key in events:
                        event = events.get(key)
                        mcno = event.get('mcno')
                        module = event.get('module')
                        eventCode = event.get('eventCode')

                        data = sharedData.get(module, mcno)

                        if not data:
                            continue

                        detector = codeToDetector.get(eventCode)

                        if not detector:
                            continue
                        
                        timestamp = datetime.datetime.now()
                        detectEvent = detector.detect(data, timestamp)

                        if not detectEvent:
                            context = Context()
                            context.timestamp = timestamp
                            context.messageType = 'event'
                            if event.get('state') == 'pending':
                                event['state'] = 'cancel'
                            else:
                                event['state'] = 'close'
                            event['endTime'] = timestamp
                            context.closeEvents.append(event)
                            eventQueue.putrr(context)
                except Exception:
                    pass
                time.sleep(self._closeEventCheckPeriod)
        except Exception as e:
            systemAlerter.sendAlert('close event abort', traceback.format_exc())
    
    def pendingEventRun(self, eventQueue, sharedData):
        systemAlerter = SystemAlerter()

        try:
            while True:
                try:
                    events = sharedData.getCategoryObj('event')

                    if not events:
                        continue

                    at = datetime.datetime.now()
                    for key in events:
                        event = events.get(key)
                        if event.get('state') != 'pending':
                            continue
                        pendingClearTime = event.get('pendingClearTime')
                        if not pendingClearTime or at > pendingClearTime:
                            event['state'] = 'event'
                            event['modify'] = True
                            event['isSend'] = True
                            context = Context()
                            context.timestamp = datetime.datetime.now()
                            context.messageType = 'event'
                            context.reopenEvents.append(event)
                            eventQueue.putrr(context)
                except Exception as e:
                    pass
                time.sleep(self._pendingEventCheckPeriod)
        except Exception as e:
            systemAlerter.sendAlert('pending event process abort', traceback.format_exc())
    
    def sleepEventRun(self, eventQueue, sharedData):
        idx = 0
        systemAlerter = SystemAlerter()

        try:
            while True:
                try:
                    events = sharedData.getCategoryObj('event')

                    if not events:
                        continue

                    at = datetime.datetime.now()

                    for key in events:
                        event = events.get(key)
                        eventCode = event.get('eventCode')
                        eventSpec = sharedData.get('eventSpec', eventCode)
                        alertTime = eventSpec.get('alertTime')

                        if event.get('state') == 'event':
                            if not TimeUtil.checkAlertTime(alertTime, at.strftime('%H%M')):
                                event['state'] = 'sleep'
                                event['modify'] = True
                                context = Context()
                                context.timestamp = event.get('startTime')
                                context.messageType = 'event'
                                context.reopenEvents.append(event)
                                eventQueue.putrr(idx, context)

                        elif event.get('state') == 'sleep':
                            if TimeUtil.checkAlertTime(alertTime, at.strftime('%H%M')):
                                event['state'] = 'event'
                                event['modify'] = True
                                context = Context()
                                context.timestamp = event.get('startTime')
                                context.messageType = 'event'
                                context.reopenEvents.append(event)
                                eventQueue.putrr(idx, context)

                except Exception:
                    pass
                time.sleep(self._sleepEventCheckPeriod)
        except Exception as e:
            systemAlerter.sendAlert('sleep event process abort', traceback.format_exc())

    def getCodeToDetector(self, sharedData):
        codeToDetector = {}

        for eventCode in DETECT_EVENT_CODES:
            obj = DETECT_EVENT_CODES[eventCode]
            if obj.get('func') == None:
                continue
            codeToDetector[eventCode] = obj.get('func')(sharedData)
        
        return codeToDetector

    def _datetimeAtToDatetime(self, datetimeAt):
        yy = int('20' + datetimeAt[:2])
        mm = int(datetimeAt[2:4])
        dd = int(datetimeAt[4:6])
        HH = int(datetimeAt[6:8])
        MM = int(datetimeAt[8:10])
        SS = int(datetimeAt[10:12])
        return datetime.datetime(yy, mm, dd, HH, MM, SS)