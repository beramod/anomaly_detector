import datetime, time, multiprocessing
from src.util.time_util import TimeUtil
from src.manager.context import Context
from src.detector.detectors.g3000.detector3001 import Detector3001
from src.detector.detectors.g3000.detector3002 import Detector3002
from src.detector.detectors.g3000.detector3003 import Detector3003
from src.detector.detectors.g3000.detector3004 import Detector3004
from src.detector.detectors.g3000.detector3005 import Detector3005
from src.detector.detectors.g3000.detector3006 import Detector3006
from src.detector.detectors.g3000.detector3007 import Detector3007
from src.detector.detectors.g3000.detector3008 import Detector3008
from src.detector.detectors.g3000.detector3009 import Detector3009
# from src.detector.detectors.g3000.detector3010 import Detector3010
from src.detector.detectors.g3000.detector3011 import Detector3011
from src.detector.detectors.g3000.detector3101 import Detector3101
from src.detector.detectors.g3000.detector3102 import Detector3102
from src.detector.detectors.g3000.detector3103 import Detector3103
from src.detector.detectors.g3000.detector3104 import Detector3104
from src.detector.detectors.g3000.detector3105 import Detector3105
from src.detector.detectors.g3000.detector3106 import Detector3106
from src.detector.detectors.g3000.detector3107 import Detector3107
from src.detector.detectors.g3000.detector3108 import Detector3108
from src.detector.detectors.g3000.detector3109 import Detector3109
from src.detector.detectors.g3000.detector3110 import Detector3110
from src.manager.shared_data import SharedData
import traceback, datetime
from src.util.system_alerter import SystemAlerter
from src.detector.detect_list import DETECT_LIST, DETECT_EVENT_CODES


class Detector:
    _closeEventCheckPeriod = 60
    _pendingEventCheckPeriod = 60
    _sleepEventCheckPeriod = 60

    def run(self, idx, rawQueue, eventQueue, sharedData):
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
                        eventQueue.put(clientId, context)

                except Exception as e:
                    messageProcErrorCnt += 1
                    if messageProcErrorCnt >= 5:
                        systemAlerter.sendAlert('detector(id: {}) message proc failed: {}'.format(
                            clientId, messageProcErrorCnt), traceback.format_exc())
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
    
    def closeEventRun(self, maxIdx, eventQueue, sharedData):
        systemAlerter = SystemAlerter()
        codeToDetector = self.getCodeToDetector(sharedData)
        idx = 0

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
                            eventSpec = sharedData.get('eventSpec', eventCode)
                            if event.get('state') == 'pending':
                                event['state'] = 'cancel'
                            else:
                                event['state'] = 'close'
                            event['endTime'] = timestamp
                            context.closeEvents.append(event)
                            eventQueue.put(idx, context)

                            idx += 1
                            if idx >= maxIdx:
                                idx = 0

                except Exception as e:
                    pass
                time.sleep(self._closeEventCheckPeriod)
        except Exception as e:
            systemAlerter.sendAlert('close event abort', traceback.format_exc())
    
    def pendingEventRun(self, maxIdx, eventQueue, sharedData):
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
                            eventQueue.put(idx, context)

                            idx += 1
                            if idx >= maxIdx:
                                idx = 0

                except Exception as e:
                    pass
                time.sleep(self._pendingEventCheckPeriod)
        except Exception as e:
            systemAlerter.sendAlert('pending event process abort', traceback.format_exc())
    
    def sleepEventRun(self, maxIdx, eventQueue, sharedData):
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
                                eventQueue.put(idx, context)

                                idx += 1
                                if idx >= maxIdx:
                                    idx = 0
                        elif event.get('state') == 'sleep':
                            if TimeUtil.checkAlertTime(alertTime, at.strftime('%H%M')):
                                event['state'] = 'event'
                                event['modify'] = True
                                context = Context()
                                context.timestamp = event.get('startTime')
                                context.messageType = 'event'
                                context.reopenEvents.append(event)
                                eventQueue.put(idx, context)
                                idx += 1
                                if idx >= maxIdx:
                                    idx = 0

                except Exception as e:
                    pass
                time.sleep(self._sleepEventCheckPeriod)
        except Exception as e:
            systemAlerter.sendAlert('sleep event process abort', traceback.format_exc())

    def getCodeToDetector(self, sharedData):
        codeToDetector = {
            3001: Detector3001(sharedData),
            3002: Detector3002(sharedData),
            3003: Detector3003(sharedData),
            3004: Detector3004(sharedData),
            3005: Detector3005(sharedData),
            3006: Detector3006(sharedData),
            3007: Detector3007(sharedData),
            3008: Detector3008(sharedData),
            3009: Detector3009(sharedData),
            3011: Detector3011(sharedData),
            3101: Detector3101(sharedData),
            3102: Detector3102(sharedData),
            3103: Detector3103(sharedData),
            3104: Detector3104(sharedData),
            3105: Detector3105(sharedData),
            3107: Detector3107(sharedData),
            3109: Detector3109(sharedData),
            3110: Detector3110(sharedData)
        }

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