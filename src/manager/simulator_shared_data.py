import time
import traceback
import datetime
from multiprocessing import Lock
from collections import defaultdict
from multiprocessing import Manager
from src.util.system_alerter import SystemAlerter
from src.util.time_util import TimeUtil
from src.util.db.soul import SoulDBCon

class SimulatorSharedData:
    def __init__(self):
        self._collectPeriod = 60
        self._worker = None
        self._data = Manager().dict()
        self._lock = Lock()

    @classmethod
    def eventKey(cls, mcno, eventCode):
        return '{}_{}'.format(eventCode, mcno)

    def getCategoryObj(self, category):
        return self._data.get(category)

    def get(self, category, key):
        try:
            if not self._data.get(category):
                return None
        except Exception as e:
            return None
        try:
            if not self._data.get(category).get(key):
                return None
        except Exception as e:
            return None
        return self._data.get(category).get(key)

    def put(self, category, key, value):
        self._lock.acquire()
        if not self._data.get(category):
            self._data[category] = {}
        temp = self._data.get(category)
        temp.update({key: value})
        self._data[category] = temp
        self._lock.release()

    def update(self, category, key, value):
        self._lock.acquire()
        if not self._data.get(category):
            self._data[category] = {}
        temp = self._data.get(category)
        temp.update({key: value})
        self._data[category] = temp
        self._lock.release()

    def delete(self, category, key):
        if not self._data.get(category):
            return
        self._lock.acquire()
        if self._data.get(category).get(key):
            temp = self._data.get(category)
            temp.pop(key)
            self._data[category] = temp
        self._lock.release()

    def getDataObj(self):
        return self._data

    def stop(self):
        self._worker.terminate()
        self._worker = None

    def init(self, mcno, sharedData):
        self._initData(mcno, sharedData)
        return sharedData

    def _initData(self, mcno, sharedData):
        metaDb = SoulDBCon.getHotDB('meta')
        alertDb = SoulDBCon.getHotDB('alert')
        self._updateData(metaDb, alertDb, mcno, sharedData)

        dataLoadTargets = self._getDataLoadTargetMcnos(metaDb)

    def _updateData(self, metaDb, alertDb, mcno, sharedData):
        self._loadEventType(metaDb, sharedData)
        self._loadEventSpec(metaDb, sharedData)
        self._loadEventAlarmSpec(metaDb, sharedData)
        self._loadEventLevelGroup(metaDb, sharedData)
        self._loadUserAlerterSetting(metaDb, sharedData)
        self._loadUsers(metaDb, sharedData)
        self._loadPowerStations(metaDb, mcno, sharedData)
        self._loadAlertOffSetting(alertDb, sharedData)
        self._loadPmsControl(metaDb, mcno, sharedData)
        self._loadCombinerBoxMeta(metaDb, sharedData)

    def _loadAlertOffSetting(self, alertDb, sharedData):
        coll = alertDb['alertOffSetting']
        now = datetime.datetime.now()
        alertOffsettings = coll.find({'reopenTime': {'$gt': now}},
                                     {'reason': True, 'reopenTime': True, 'uuid': True,
                                      'eventCodes': True, 'mcnos': True, 'containsMessages': True})
        alertOffSettingMap = {'0': []}
        if alertOffsettings:
            for _, alertOffSetting in enumerate(alertOffsettings):
                settingInfo = {
                    'reason': alertOffSetting.get('reason'),
                    'containsMessages': alertOffSetting.get('containsMessages'),
                    'uuid': alertOffSetting.get('uuid'),
                    'reopenTime': alertOffSetting.get('reopenTime'),
                    'eventCodes': alertOffSetting.get('eventCodes')
                }
                mcnos = alertOffSetting.get('mcnos')
                if len(mcnos) == 0:
                    alertOffSettingMap['0'].append(settingInfo)
                else:
                    for mcno in mcnos:
                        if alertOffSettingMap.get(mcno):
                            alertOffSettingMap.get(mcno).append(settingInfo)
                        else:
                            alertOffSettingMap[mcno] = [settingInfo]
            sharedData['alertOffSetting'] = alertOffSettingMap

    def _loadEventType(self, metaDb, sharedData):
        coll = metaDb['codes']
        codes = coll.find({'codeGroup': 'alert.event.type'}, {'_id': False, 'codeName': True, 'code': True})
        eventTypeMap = {}
        if codes:
            for _, each in enumerate(codes):
                eventTypeMap[each.get('code')] = each.get('codeName')
            sharedData['eventType'] = eventTypeMap

    def _loadEventSpec(self, metaDb, sharedData):
        coll = metaDb['eventSpec']
        eventSpecs = coll.find({}, {'_id': False, 'createdAt': False})
        eventSpecMap = {}
        if eventSpecs:
            for _, each in enumerate(eventSpecs):
                eventSpecMap[each.get('eventCode')] = each
            sharedData['eventSpec'] = eventSpecMap

    def _loadEventAlarmSpec(self, metaDb, sharedData):
        coll = metaDb['eventAlarmSpec']
        eventAlarmSpecs = coll.find({'eventCode': {'$ne': None}}, {'_id': False, 'alarmId': True, 'eventCode': True})
        eventSpecMap = {}
        if eventAlarmSpecs:
            for _, each in enumerate(eventAlarmSpecs):
                eventSpecMap[each.get('eventCode')] = each
            sharedData['eventAlarmSpec'] = eventSpecMap

    def _loadEventLevelGroup(self, metaDb, sharedData):
        coll = metaDb['codes']
        eventLevelGroups = coll.find({'codeGroup': 'alert.level.group'}, {'_id': False, 'levels': True, 'code': True})
        eventLevelGroupMap = {}
        if eventLevelGroups:
            for _, each in enumerate(eventLevelGroups):
                eventLevelGroupMap[each.get('code')] = each.get('levels')
            sharedData['eventLevelGroup'] = eventLevelGroupMap

    def _loadModuleEventSpec(self, metaDb, sharedData):
        coll = metaDb['moduleEventSpec']
        moduleEventSpecs = coll.find({}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        moduleEventSpecMap = {}
        if moduleEventSpecs:
            for _, each in enumerate(moduleEventSpecs):
                brand = each.get('brand')
                version = each.get('version')
                moduleEventSpecMap['{}_{}'.format(brand, version)] = each
            sharedData['moduleEventSpec'] = moduleEventSpecMap

    def _loadUserAlerterSetting(self, metaDb, sharedData):
        coll = metaDb['userAlerterSetting']
        userAlerterSettings = coll.find({'syncedAt': {'$exists': False}},
                                        {'_id': False, 'createdAt': False, 'updatedAt': False, 'syncedAt': False})
        if userAlerterSettings:
            userAlerterSettingMap = self._makeMap(userAlerterSettings, 'userId')
            sharedData['userAlerterSetting'] = userAlerterSettingMap

    def _loadUsers(self, metaDb, sharedData):
        coll = metaDb['users']
        # TODO: 희성에서 Sync 되어 넘어 온 유저는 아직 경보 발송 X -> 추후 서비스 통합 완료되면 변경
        users = list(coll.find({'syncedAt': {'$exists': False}, 'deleted': False},
                               {'_id': False, 'userId': True, 'authorityCode': True, 'mcnos': True, 'email': True}))

        userMap = self._makeMap(users, 'userId')
        mcnoToUserMap = defaultdict(list)
        if users:
            for _, each in enumerate(users):
                for mcnoObj in each.get('mcnos'):
                    mcnoToUserMap[mcnoObj.get('mcno')].append(each)
            sharedData['user'] = userMap
            sharedData['mcnoToUser'] = mcnoToUserMap
            sharedData['admin'] = list(filter(lambda el: el.get('authorityCode') >= 4, list(userMap.values())))

    def _loadPowerStations(self, metaDb, mcno, sharedData):
        coll = metaDb['powerStations']
        powerStations = coll.find({'syncedAt': {'$exists': False}, 'mcno': mcno},
                                  {'_id': False, 'mcno': True, 'psStatusCode': True, 'psName': True,
                                   'manage.onm': True})
        if powerStations:
            powerStationMap = self._makeMap(powerStations, 'mcno')
            sharedData['powerStation'] = powerStationMap

    def _loadVirtualPs(self, metaDb, sharedData):
        coll = metaDb['virtualPs']
        virtualPs = coll.find({}, {'_id': False, 'document': False})
        if virtualPs:
            virtualPsMap = self._makeMap(virtualPs, 'mcno')
            sharedData['virtualPs'] = virtualPsMap

    def _loadEvent(self, alertDb, sharedData):
        coll = alertDb['events']
        events = coll.find({}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        eventMap = {}
        if events:
            for _, each in enumerate(events):
                mcno = each.get('mcno')
                eventCode = each.get('eventCode')
                eventMap[SimulatorSharedData.eventKey(mcno, eventCode)] = each
        sharedData['event'] = eventMap

    def _loadInverter(self, mcnos, inverterDb, sharedData):
        coll = inverterDb['inverters']
        inverters = coll.find({'mcno': {'$in': mcnos}}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if inverters:
            inverterMap = self._makeMap(inverters, 'mcno')
            sharedData['inverter'] = inverterMap

    def _loadPcs(self, mcnos, pcsDb, sharedData):
        coll = pcsDb['pcs']
        pcs = coll.find({'mcno': {'$in': mcnos}}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if pcs:
            pcsMap = self._makeMap(pcs, 'mcno')
            sharedData['pcs'] = pcsMap

    def _loadBms(self, mcnos, bmsDb, sharedData):
        coll = bmsDb['bms']
        bms = coll.find({'mcno': {'$in': mcnos}}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if bms:
            bmsMap = self._makeMap(bms, 'mcno')
            sharedData['bms'] = bmsMap

    def _loadMeter(self, mcnos, meterDb, sharedData):
        coll = meterDb['meters']
        meters = coll.find({'mcno': {'$in': mcnos}}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if meters:
            meterMap = self._makeMap(meters, 'mcno')
            sharedData['meter'] = meterMap

    def _loadIsoMeter(self, mcnos, isoMeterDb, sharedData):
        coll = isoMeterDb['isoMeters']
        meters = coll.find({'mcno': {'$in': mcnos}}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if meters:
            meterMap = self._makeMap(meters, 'mcno')
            sharedData['isoMeter'] = meterMap

    def _loadPms(self, mcnos, pmsDb, sharedData):
        coll = pmsDb['pms']
        pms = coll.find({'mcno': {'$in': mcnos}}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if pms:
            pmsMap = self._makeMap(pms, 'mcno')
            sharedData['pms'] = pmsMap

    def _loadEmsEnv(self, mcnos, emsEnvDb, sharedData):
        coll = emsEnvDb['emsEnv']
        emsEnvs = coll.find({'mcno': {'$in': mcnos}}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if emsEnvs:
            emsEnvMap = self._makeMap(emsEnvs, 'mcno')
            sharedData['emsEnv'] = emsEnvMap

    def _loadPmsControl(self, metaDB, mcno, sharedData):
        coll = metaDB['pmsControl']
        pmsControls = coll.find({'mcno': mcno}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if pmsControls:
            pmsControlMap = self._makeMap(pmsControls, 'mcno')
            sharedData['pmsControl'] = pmsControlMap

    def _makeMap(self, dataList, key):
        resultMap = {}
        for _, each in enumerate(dataList):
            resultMap[each.get(key)] = each
        return resultMap

    def _loadWeather(self, mcnos, weatherDb, sharedData):
        coll = weatherDb['weather']
        weather = coll.find({'mcno': {'$in': mcnos}}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if weather:
            weatherMap = self._makeMap(weather, 'mcno')
            sharedData['weather'] = weatherMap

    def _loadCombinerBox(self, mcnos, combinerBoxDb, sharedData):
        coll = combinerBoxDb['combinerBox']
        combinerBox = coll.find({'mcno': {'$in': mcnos}}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if combinerBox:
            combinerBoxMap = self._makeMap(combinerBox, 'mcno')
            sharedData['combinerBox'] = combinerBoxMap

    def _getDataLoadTargetMcnos(self, metaDb):
        coll = metaDb['powerStations']
        powerStations = coll.find({'syncedAt': {'$exists': False}}, {'_id': False, 'mcno': True})
        mcnos = []
        for _, each in enumerate(powerStations):
            mcno = each.get('mcno')
            mcnos.append(mcno)
        return mcnos

    def _loadCombinerBoxMeta(self, metaDb, sharedData):
        coll = metaDb['combinerBoxMeta']
        combinerBox = coll.find({}, {'_id': False, 'createdAt': False, 'updatedAt': False})
        if combinerBox:
            combinerBoxMap = self._makeMap(combinerBox, 'mcno')
            sharedData['combinerBoxMeta'] = combinerBoxMap