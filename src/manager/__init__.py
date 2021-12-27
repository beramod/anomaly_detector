import multiprocessing, time, traceback
from src.filter import Filter
from src.detector import Detector
from .queue import AnomalyDetectorQueue
from .shared_data import SharedData
from multiprocessing import Process, Manager
from src.alerter import Alerter
from src.consumer.consumer import AnomalyDetectorConsumer
from src.define import *


class AnomalyDetectorManager:
    def __init__(self):
        self._consumerGroups = {
            'pcs': {
                'topic': 'PcsMin',
                'consumers': []
            },
            'pms': {
                'topic': 'PmsMin',
                'consumers': []
            },
            'bms': {
                'topic': 'BmsMin',
                'consumers': []
            },
            'meter': {
                'topic': 'MeterMin',
                'consumers': []
            },
            'inverter': {
                'topic': 'InverterMin',
                'consumers': []
            },
            'event': {
                'topic': 'Event',
                'consumers': []
            },
            'weather': {
                'topic': 'WeatherMin',
                'consumers': []
            },
            'machineIndex': {
                'topic': 'MachineIndexMin',
                'consumers': []
            },
            'combinerBox': {
                'topic': 'CombinerBoxMin',
                'consumers': []
            },
            'isoMeter': {
                'topic': 'IsoMeterMin',
                'consumers': []
            }
        }

        self._workers = []

        self._rawQueue = AnomalyDetectorQueue()
        self._eventQueue = AnomalyDetectorQueue()
        self._alertQueue = AnomalyDetectorQueue()

        self._logger = multiprocessing.get_logger()

        self._alerter = Alerter()
        self._sharedData = SharedData()

    def _init(self):
        rawConsumers = []
        for topic in ['PcsMin', 'PmsMin', 'BmsMin', 'MeterMin', 'InverterMin', 'EmsEnvMin', 'WeatherMin', 'MachineIndexMin', 'CombinerBoxMin', 'IsoMeterMin']:
            rawConsumers.extend(self._createConsumer(topic, '{}ADGroup'.format(topic)))
        
        eventConsumers = self._createConsumer('Event', 'EventADGroup')
        detectors = self._createDetectors()
        filters = self._createFilters()
        alerters = self._createAlerters()

        return {
            'rawConsumers': rawConsumers,
            'eventConsumers': eventConsumers,
            'detectors': detectors,
            'filters': filters,
            'alerters': alerters
        }

    def run(self):
        processObjs = self._init()
        workers = []
        s = time.time()
        try:
            print('Init shared data...')
            self._sharedData.init(self._sharedData.getDataObj())
            print('Complete init shared data! time: {} sec'.format(time.time() - s))
            worker = Process(target=self._sharedData.run, args=[self._sharedData])
            worker.start()
            workers.append(worker)
            for idx, process in enumerate(processObjs.get('rawConsumers')):
                worker = Process(target=process.run, args=[self._rawQueue, self._sharedData])
                worker.start()
                workers.append(worker)
                print('[rawConsumers] run worker({}) {}/{}'.format(worker.pid, idx + 1, len(processObjs.get('rawConsumers'))))

            for idx, process in enumerate(processObjs.get('eventConsumers')):
                worker = Process(target=process.run, args=[self._eventQueue, self._sharedData])
                worker.start()
                workers.append(worker)
                print('[eventConsumers] run worker({}) {}/{}'.format(worker.pid, idx + 1, len(processObjs.get('eventConsumers'))))

            for idx, process in enumerate(processObjs.get('detectors')):
                worker = Process(target=process.run, args=[idx, self._rawQueue, self._sharedData])
                worker.start()
                workers.append(worker)
                print('[detectors] run worker({}) {}/{}'.format(worker.pid, idx + 1, len(processObjs.get('detectors'))))

            closeEventDetector = Detector()
            closeEventWorker = Process(target=closeEventDetector.closeEventRun, args=[self._eventQueue, self._sharedData])
            closeEventWorker.start()
            workers.append(closeEventWorker)
            print('[close event manager] run worker({})'.format(closeEventWorker.pid))

            pendingEventDetector = Detector()
            pendingEventWorker = Process(target=pendingEventDetector.pendingEventRun, args=[self._eventQueue, self._sharedData])
            pendingEventWorker.start()
            workers.append(pendingEventWorker)
            print('[pending event manager] run worker({})'.format(pendingEventWorker.pid))

            sleepEventDetector = Detector()
            sleepEventWorker = Process(target=sleepEventDetector.sleepEventRun, args=[self._eventQueue, self._sharedData])
            sleepEventWorker.start()
            workers.append(sleepEventWorker)
            print('[sleep event manager] run worker({})'.format(sleepEventWorker.pid))

            for idx, process in enumerate(processObjs.get('filters')):
                worker = Process(target=process.run, args=[idx, self._eventQueue, self._alertQueue, self._sharedData])
                worker.start()
                workers.append(worker)
                print('[filters] run worker({}) {}/{}'.format(worker.pid, idx + 1, len(processObjs.get('filters'))))

            for idx, process in enumerate(processObjs.get('alerters')):
                worker = Process(target=process.run, args=[idx, self._alertQueue, self._sharedData])
                worker.start()
                workers.append(worker)
                print('[alerters] run worker({}) {}/{}'.format(worker.pid, idx + 1, len(processObjs.get('alerters'))))

            print('run completed!')
            print('Total Workers: {}'.format(len(workers)))

            self._workers = workers
            self._join()
        except Exception as e:
            print(traceback.format_exc())

    def _createConsumer(self, topic, groupId, processCnt = 6):
        consumers = []
        for idx in range(0, processCnt):
            consumer = AnomalyDetectorConsumer(topic, groupId, idx)
            consumers.append(consumer)
        return consumers

    def _createDetectors(self, processCnt = 6):
        detectors = []
        for idx in range(0, processCnt):
            detector = Detector()
            detectors.append(detector)
        return detectors

    def _createFilters(self, processCnt = 6):
        filters = []
        for idx in range(0, processCnt):
            filter = Filter()
            filters.append(filter)
        return filters

    def _createAlerters(self, processCnt = 6):
        alerters = []
        for idx in range(0, processCnt):
            alerter = Alerter()
            alerters.append(alerter)
        return alerters

    def _join(self):
        for worker in self._workers:
            worker.join()
