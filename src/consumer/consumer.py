import time, json, datetime, traceback
from src.manager.context import Context
from confluent_kafka import Consumer
from src.util.system_alerter import SystemAlerter
from src.util.time_util import TimeUtil
from src.define import *

class AnomalyDetectorConsumer:
    def __init__(self, topic, groupId, clientId):
        self._servers = '{broker ip list}'
        self._topic = topic
        self._groupId = groupId
        self._clientId = clientId
        self._autoCommit = True
        self._autoCommitIntervalMs = 5000
        self._autoOffsetReset = 'smallest'
        self._compressionType = 'lz4'
        self._consumerObj = None
        self._consumeErrorCnt = 0
        self._messageProcErrorCnt = 0
        self._systemAlerter = None

    def _init(self):
        self._systemAlerter = SystemAlerter()
        self._consumerObj = Consumer({
            'bootstrap.servers': self._servers,
            'group.id': self._groupId,
            'client.id': '{}-{}'.format(self._groupId, self._clientId),
            'enable.auto.commit': self._autoCommit,
            'auto.commit.interval.ms': self._autoCommitIntervalMs,
            'auto.offset.reset': self._autoOffsetReset,
            'compression.type': self._compressionType
        })
        self._consumerObj.subscribe([self._topic])

    def run(self, queue, sharedData):
        self._init()

        try:
            while True:
                message = self._consumerObj.poll(3.0)

                if message is None:
                    continue
                elif message.error():
                    self._consumeErrorCnt += 1
                    if self._consumeErrorCnt >= 5:
                        self._systemAlerter.sendAlert('KAFKA Consumer Error: {}, topic: {}, clientId: {}'
                                          .format(self._consumeErrorCnt, self._topic, self._clientId),
                                        message.error().str())
                    continue

                self._consumeErrorCnt = 0
                self._messageProcErrorCnt = 0

                res = message.value().decode('utf-8')

                try:
                    kafkaMessage = json.loads(res)
                    context = Context()
                    context.parse(kafkaMessage)

                    if context.messageType == 'event':
                        dataObj = kafkaMessage.get('data')
                        TimeUtil.eventStrTimeToTime(dataObj)
                        context.openEvents.append(dataObj)
                    elif context.messageType in COLLECT_MODULES:
                        oldData = sharedData.get(context.messageType, context.raw.get('mcno'))
                        if ((oldData is not None) and (oldData.get('datetimeAt') <= context.raw.get('datetimeAt'))):
                            sharedData.update(context.messageType, context.raw.get('mcno'), context.raw)
                    queue.put(self._clientId, context)

                except Exception as e:
                    self._messageProcErrorCnt += 1
                    if self._messageProcErrorCnt >= 5:
                        self._systemAlerter.sendAlert('Message Proc Error {}, topic: {}, clientId: {}'
                                          .format(self._messageProcErrorCnt, self._topic, self._clientId),
                                        traceback.format_exc())
                time.sleep(0.005)

        except Exception as e:
            self._systemAlerter.sendAlert('anomaly detector consumer abort. topic: {}, clientId: {}'.format(self._topic, self._clientId), traceback.format_exc())

    def stop(self):
        self._consumerObj.close()
