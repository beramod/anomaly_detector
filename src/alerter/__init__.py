import json, traceback, time, datetime, multiprocessing
from src.manager.shared_data import SharedData
from confluent_kafka import Producer
from src.util.system_alerter import SystemAlerter
from src.util.time_util import TimeUtil
from src.define import *
from src.util.db.soul import SoulDBCon
from pymongo import IndexModel

LOGGER = multiprocessing.get_logger()


class Alerter:
    def run(self, idx, alerterQueue, sharedData):
        clientId = idx
        while True:
            try:
                systemAlerter = SystemAlerter()
                kafkaProducer = Producer({
                    'bootstrap.servers': 'broker ip list',
                    'compression.type': 'lz4'
                })
                messageProcErrorCnt = 0
                alertDb = SoulDBCon.getHotDB('alert')
                while True:
                    try:
                        context = alerterQueue.get(clientId)

                        if context == None:
                            messageProcErrorCnt = 0
                            pass

                        messageProcErrorCnt = 0
                        self._processOpenEvent(context, sharedData, kafkaProducer, alertDb)
                        self._processCloseEvent(context, sharedData, kafkaProducer, alertDb)
                        self._processUpdateEvent(context, sharedData, kafkaProducer, alertDb)
                        self._processReopenEvent(context, sharedData, kafkaProducer, alertDb)
                        self._processNotis(context, sharedData, kafkaProducer)
                        del context

                    except Exception as e:
                        messageProcErrorCnt += 1
                        if messageProcErrorCnt >= 5:
                            systemAlerter.sendAlert('Alerter Proc Error {}'.format(messageProcErrorCnt),
                                                          traceback.format_exc())
            except Exception as e:
                systemAlerter.sendAlert('Alerter Abort: {}', traceback.format_exc())

    def _processOpenEvent(self, context, sharedData, kafkaProducer, alertDb):
        messages = []

        for openEvent in context.openEvents:
            state = openEvent.get('state')
            targets = None
            if state == 'event':
                targets = self._getTargets(openEvent, sharedData)
                openEvent['isSend'] = True
            if targets:
                messages.append(self._createMessageDoc(openEvent, targets, 'alerton'))
            if openEvent.get('data') and openEvent.get('data').get('option') \
                    and openEvent.get('data').get('option').get('isSendSoulmate'):
                self._sendSoulmate('open', openEvent, kafkaProducer)

        for message in messages:
            self._sendMessage('Message', message, kafkaProducer)

        openEvents = list(filter(lambda el: not el.get('modify') and el.get('state') != 'delete', context.openEvents))
        updateEvents = list(filter(lambda el: el.get('modify'), context.openEvents))

        if context.store:
            if openEvents:
                self._insertEvents(openEvents, alertDb, sharedData)

        for event in updateEvents:
            self._updateEvent(event.get('eventCode'), event.get('mcno'), event, alertDb, sharedData)
            time.sleep(0.001)
    
    def _processCloseEvent(self, context, sharedData, kafkaProducer, alertDb):
        messages = []
        for closeEvent in context.closeEvents:
            state = closeEvent.get('state')
            targets = None
            if state == 'close' and closeEvent.get('isSend'):
                targets = self._getTargets(closeEvent, sharedData)
            if targets:
                messages.append(self._createMessageDoc(closeEvent, targets, 'alertoff'))
            if closeEvent.get('data') and closeEvent.get('data').get('option') \
                    and closeEvent.get('data').get('option').get('isSendSoulmate'):
                self._sendSoulmate('close', closeEvent, kafkaProducer)

        for message in messages:
            self._sendMessage('Message', message, kafkaProducer)
        if context.closeEvents:
            self._closeEvents(context.closeEvents, alertDb, sharedData)

    def _processUpdateEvent(self, context, sharedData, kafkaProducer, alertDb):
        for event in context.events:
            if event.get('modify'):
                self._updateEvent(event.get('eventCode'), event.get('mcno'), event, alertDb, sharedData)
                if event.get('data') and event.get('data').get('option') \
                        and event.get('data').get('option').get('isSendSoulmate'):
                    self._sendSoulmate('update', event, kafkaProducer)
                time.sleep(0.001)

    def _processReopenEvent(self, context, sharedData, kafkaProducer, alertDb):
        messages = []
        for reopenEvent in context.reopenEvents:
            state = reopenEvent.get('state')
            targets = None
            if state == 'event':
                targets = self._getTargets(reopenEvent, sharedData)
            if targets:
                messages.append(self._createMessageDoc(reopenEvent, targets, 'alerton'))
            if reopenEvent.get('modify'):
                self._updateEvent(reopenEvent.get('eventCode'), reopenEvent.get('mcno'), reopenEvent, alertDb, sharedData)
                if reopenEvent.get('data') and reopenEvent.get('data').get('option') \
                        and reopenEvent.get('data').get('option').get('isSendSoulmate'):
                    self._sendSoulmate('reopen', reopenEvent, kafkaProducer)
                time.sleep(0.001)
        for message in messages:
            self._sendMessage('Message', message, kafkaProducer)
    
    def _processNotis(self, context, sharedData, kafkaProducer):
        messages = []
        for noti in context.notis:
            targets = self._getTargets(noti, sharedData)
            if targets:
                messages.append(self._createMessageDoc(noti, targets, 'noti'))
        for message in messages:
            self._sendMessage('Message', message, kafkaProducer)

    def _createMessageDoc(self, event, targets, messageType):
        isOpen = True

        title = '[{}-{}]'.format(event.get('psName'), event.get('mcno'))
        body = '{}({}){}\n'.format(event.get('eventName'), event.get('eventCode') , '' if isOpen else ' / 해제')

        if event.get('message'):
            body += event.get('message')

        if event.get('state') == 'event':
            body += '\n발생시간: {}'.format(event.get('startTime').strftime('%y/%m/%d %H:%M'))
        elif event.get('state') == 'close':
            isOpen = False
            body += '\n지속시간: {} ~ {}'.format(
                event.get('startTime').strftime('%y/%m/%d %H:%M'), 
                event.get('endTime').strftime('%y/%m/%d %H:%M'))

        message = '{}\n{}'.format(title, body)

        messageDoc = {
            'messageId': None,
            'messageType': messageType,
            'title': title,
            'content': message,
            'reservationTime': None,
            'targets': targets,
            'data': event.get('data')
        }

        messageDoc['data']['eventId'] = event.get('eventId')
        
        return messageDoc

    def _getTargets(self, event, sharedData):
        eventCode = event.get('eventCode')
        mcno = event.get('mcno')

        if mcno > 99999:
            return self._createVirtualTarget(sharedData.get('virtualPs', mcno))

        admins = sharedData.getCategoryObj('admin')
        eventSpec = sharedData.get('eventSpec', eventCode)
        userAlerterSettingMap = sharedData.getCategoryObj('userAlerterSetting')
        channel = eventSpec.get('channel')
        psUsers = sharedData.get('mcnoToUser', mcno)
        powerStationInfo = sharedData.get('powerStation', mcno)
        alertOffSettings = sharedData.get('alertOffSetting', mcno)
        if alertOffSettings is None:
            alertOffSettings = sharedData.get('alertOffSetting', '0')
        else:
            obj = sharedData.get('alertOffSetting', '0')
            if obj is not None:
                alertOffSettings.extend(obj)
        
        if not powerStationInfo:
            return []

        psStatusCode = powerStationInfo.get('psStatusCode')

        if not psUsers:
            psUsers = []

        psUsers.extend(admins)
        psUsers = list({psUser['userId']: psUser for psUser in psUsers}.values())
        targets = []

        if psStatusCode == PS_STATUS_INSTALLING:
            return targets

        alertOff = self._checkAlertOff(alertOffSettings, event, eventSpec)

        if alertOff:
            return targets

        for each in psUsers:
            authorityCode = each.get('authorityCode')
            userId = each.get('userId')
            userAlerterSetting = userAlerterSettingMap.get(userId)

            # if authorityCode == 1:
            #     continue
            if userAlerterSetting is None:
                continue
            if userAlerterSetting.get('alertOn') == False:
                continue

            userAlerterSpec = self._getUserAlerterSpec(userAlerterSetting, mcno, eventSpec.get('eventCode'))

            if not userAlerterSpec:
                continue

            if 1001 <= eventCode <= 1004 and userAlerterSpec.get('noticeOnEvent') == False:
                continue
            
            if 2000 <= eventCode and userAlerterSpec.get('noticeOnEvent') == False:
                continue
                
            if 1200 <= eventCode < 1400 and userAlerterSpec.get('noticeOnManage') == False:
                continue
                
            if 1400 <= eventCode < 1999 and userAlerterSpec.get('noticeOnFinance') == False:
                continue

            isTarget = self._isTarget(userId, eventSpec, authorityCode)
            
            if isTarget:
                isAdmin = (authorityCode >= AUTHORITY_ADMIN_LEVEL_MIN)
                onSms = userAlerterSpec.get('channel').get('sms')
                onMail = userAlerterSpec.get('channel').get('mail')
                onPush = userAlerterSpec.get('channel').get('push')
                onKakao = userAlerterSpec.get('channel').get('kakao')
                onSlack = channel.get('slack')

                if channel.get('sms') and onSms:
                    targets.extend(self._createTargetSms(userAlerterSetting))
                if channel.get('mail') and onMail:
                    mail = each.get('email')
                    if mail:
                        targets.append(self._createTarget('mail', mail, userId))
                if channel.get('push') and onPush:
                    targets.extend(self._createTargetPush(userAlerterSetting))
                if channel.get('kakao') and onKakao:
                    targets.extend(self._createTargetAlimtalk(userAlerterSetting))
                if onSlack:
                    targets.append(self._createTarget('slack', 'ems_alert', userId))
        
        return targets

    def _createVirtualTarget(self, virtualPs):
        if not virtualPs:
            return []

        targets = []

        for phone in virtualPs.get('owners'):
            targets.append(self._createTarget('kakao', phone, 'virtualAlert'))
        return targets

    def _checkAlertOff(self, alertOffSettings, event, eventSpec):
        if alertOffSettings is None or event is None or eventSpec is None:
            return False
        
        eventType = int(str(event.get('eventCode'))[:1]+'000')
        alertOffSettingObjs = list(filter(lambda el: 
            event.get('eventCode') in el.get('eventCodes') or eventType in el.get('eventCodes')
            , alertOffSettings))

        if not alertOffSettingObjs:
            return False
        
        now = datetime.datetime.now()

        result = False

        for each in alertOffSettingObjs:
            repoenTime = each.get('reopenTime')
            if now > repoenTime:
                continue

            if each.get('containsMessages'):
                for containsMessage in each.get('containsMessages'):
                    if containsMessage in eventSpec.get('eventDesc') or\
                        containsMessage in event.get('message'):
                        result = True
                        break
            else:
                result = True
                break
        
        return result
    
    def _isTarget(self, userId, eventSpec, authorityCode):
        authoGte = eventSpec.get('authority').get('gte')
        authoLte = eventSpec.get('authority').get('lte')
        authoIs = eventSpec.get('authority').get('is')
        userIdIn = eventSpec.get('authority').get('in')

        isTarget = True

        if authoGte and authoGte > authorityCode:
            isTarget = False
        if authoLte and authoLte < authorityCode:
            isTarget = False
        if authoIs and authoIs == authorityCode:
            isTarget = True
        if userId in userIdIn:
            isTarget = True

        return isTarget

    def _getUserAlerterSpec(self, userAlerterSetting, mcno, eventCode):
        alerterSetting = None
        for alerterSettingObj in userAlerterSetting.get('alerterSetting'):
            if mcno == alerterSettingObj.get('mcno'):
                alerterSetting = alerterSettingObj
                break
        if not alerterSetting:
            return None
        userAlerterSpec = None
        for alerterObj in alerterSetting.get('alerter'):
            if eventCode == alerterObj.get('eventCode'):
                userAlerterSpec = alerterObj
                break
        if userAlerterSpec is None:
            return None
        userAlerterSpec['noticeOnEvent'] = alerterSetting.get('noticeOnEvent')
        userAlerterSpec['noticeOnManage'] = alerterSetting.get('noticeOnManage')
        userAlerterSpec['noticeOnFinance'] = alerterSetting.get('noticeOnFinance')
        return userAlerterSpec

    def _createTargetSms(self, userAlerterSetting):
        targets = []
        userId = userAlerterSetting.get('userId')
        phones = userAlerterSetting.get('phones')
        for phoneObj in phones:
            if phoneObj.get('alertOn'):
                targets.append(self._createTarget('sms', phoneObj.get('phone'), userId))
        return targets

    def _createTargetPush(self, userAlerterSetting):
        targets = []
        userId = userAlerterSetting.get('userId')
        tokens = userAlerterSetting.get('appTokens')
        for tokenObj in tokens:
            if tokenObj.get('alertOn'):
                targets.append(self._createTarget('push', tokenObj.get('token'), userId))
        return targets
    
    def _createTargetAlimtalk(self, userAlerterSetting):
        targets = []
        userId = userAlerterSetting.get('userId')
        phones = userAlerterSetting.get('phones')
        for phoneObj in phones:
            if phoneObj.get('alertOn'):
                targets.append(self._createTarget('kakao', phoneObj.get('phone'), userId))
        return targets

    def _createTarget(self, channel, to, userId):
        return {
            'userId': userId,
            'channel': channel,
            'to': to,
            'result': None
        }

    def _sendMessage(self, topic, message, kafkaProducer):
        kafkaMeesageDoc = {
            'messageType': 'message',
            'timestamp': int(time.time()),
            'data': message
        }
        
        if not message.get('messageType'):
            message['messageType'] = 'alert'
        
        try:
            kafkaProducer.produce(topic, json.dumps(kafkaMeesageDoc))
            kafkaProducer.poll(timeout=0.3)
            return True
        except Exception as e:
            return False

    def _sendSoulmate(self, messageType, message, kafkaProducer):
        TimeUtil.eventTimeToStr(message)
        kafkaMeesageDoc = {
            'messageType': messageType,
            'timestamp': int(time.time()),
            'data': message
        }

        try:
            kafkaProducer.produce('SoulMateEvent', json.dumps(kafkaMeesageDoc))
            kafkaProducer.poll(timeout=0.3)
            return True
        except Exception as e:
            return False

    def _insertEvents(self, insertEvents, alertDb, sharedData):
        for event in insertEvents:
            if event.get('modify'):
                event.pop('modify')
            if type(event.get('startTime')) == str:
                event['startTime'] = TimeUtil.strTimeToTime(event.get('startTime'))
            if type(event.get('endTime')) == str:
                event['endTime'] = TimeUtil.strTimeToTime(event.get('endTime'))
            if type(event.get('pendingClearTime')) == str:
                event['pendingClearTime'] = TimeUtil.strTimeToTime(event.get('pendingClearTime'))
            
        if insertEvents:
            res = self._insertDataToHotDb(alertDb, 'events', insertEvents)
            if not res:
                LOGGER.info('FAIL INSERT EVENT')
                return
        try:
            for event in insertEvents:
                key = SharedData.eventKey(event.get('mcno'), event.get('eventCode'))
                sharedData.put('event', key, event)
        except Exception:
            print('FAIL INSERT EVENT: {}'.format(traceback.format_exc()))
            LOGGER.info('FAIL INSERT EVENT: {}'.format(traceback.format_exc()))

    def _updateEvent(self, eventCode, mcno, updateEvent, alertDb, sharedData):
        if updateEvent.get('modify'):
            updateEvent.pop('modify')
        if type(updateEvent.get('startTime')) == str:
            updateEvent['startTime'] = TimeUtil.strTimeToTime(updateEvent.get('startTime'))
        if type(updateEvent.get('endTime')) == str:
            updateEvent['endTime'] = TimeUtil.strTimeToTime(updateEvent.get('endTime'))
        if type(updateEvent.get('pendingClearTime')) == str:
            updateEvent['pendingClearTime'] = TimeUtil.strTimeToTime(updateEvent.get('pendingClearTime'))

        res = self._updateDataToHotDb(alertDb, 'events', {'mcno': mcno, 'eventCode': eventCode}, updateEvent)
        if not res:
            return

        key = SharedData.eventKey(updateEvent.get('mcno'), updateEvent.get('eventCode'))
        sharedData.update('event', key, updateEvent)

    def _closeEvents(self, closeEvents, alertDb, sharedData):
        for event in closeEvents:
            if event.get('delete'):
                event.pop('delete')
            if type(event.get('startTime')) == str:
                event['startTime'] = TimeUtil.strTimeToTime(event.get('startTime'))
            if type(event.get('endTime')) == str:
                event['endTime'] = TimeUtil.strTimeToTime(event.get('endTime'))
            if type(event.get('pendingClearTime')) == str:
                event['pendingClearTime'] = TimeUtil.strTimeToTime(event.get('pendingClearTime'))

        if closeEvents:
            res = self._removeDataFromHotDb(alertDb, 'events', closeEvents)
            if not res:
                return
            yymm = datetime.datetime.now().strftime('%y%m')
            logRes = self._insertDataToHotDb(alertDb, f'eventLog_{yymm}', closeEvents, isLog=True)
            if not logRes:
                return

        for event in closeEvents:
            key = SharedData.eventKey(event.get('mcno'), event.get('eventCode'))
            sharedData.delete('event', key)

    def _insertDataToHotDb(self, db, collName, docs, isProc=False, isLog=False):
        coll = db[collName]
        now = datetime.datetime.now()
        for doc in docs:
            if doc.get('createdAt') is not None:
                doc.pop('createdAt')
            if not isLog:
                doc['createdAt'] = now
            doc['updatedAt'] = now
        try:
            if not isProc and (collName not in db.collection_names()):
                unique = False
                if not isLog:
                    unique = True
                model1 = IndexModel([('eventCode', 1)], name='eventCode')
                model2 = IndexModel([('mcno', 1)], name='mcno')
                model3 = IndexModel([('state', 1)], name='state')
                model4 = IndexModel([('eventCode', 1), ("mcno", 1)], name="eventCode_mcno", unique=unique)
                coll.create_indexes([model1, model2, model3, model4])

            coll.insert(docs)
            return True
        except Exception as e:
            return False

    def _updateDataToHotDb(self, db, collName, target, updateDoc):
        coll = db[collName]
        now = datetime.datetime.now()
        updateDoc['updatedAt'] = now
        try:
            coll.update(target, {'$set': updateDoc})
            return True
        except Exception as e:
            return False

    def _removeDataFromHotDb(self, db, collName, removeDocs):
        deleteQuery = {
            '$or': []
        }
        for doc in removeDocs:
            deleteQuery['$or'].append({
                'mcno': doc.get('mcno'),
                'eventCode': doc.get('eventCode')
            })

        coll = db[collName]
        try:
            coll.remove(deleteQuery)
            return True
        except Exception as e:
            return False