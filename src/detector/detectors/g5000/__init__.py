from src.detector.detectors import BaseDetector


class Detector5xxx(BaseDetector):
    def _parseFaults(self, data, moduleName, moduleNoName, level):
        moduleEvents = []
        message = ''
        existStatusCode = []
        modulenos = []
        levelGroup = self._sharedData.get('eventLevelGroup', level)

        for each in data.get(moduleName):
            moduleno = each.get(moduleNoName)
            faults = each.get('faults')
            if not faults:
                continue
            for each in faults:
                each['moduleNo'] = moduleno
                if each.get('eventLevel') in levelGroup:
                    if each.get('statusCode') not in existStatusCode:
                        moduleEvents.append(each)
                    if moduleno not in modulenos:
                        modulenos.append(moduleno)

        if len(moduleEvents) == 0:
            return {'detect': False}

        message = ''

        for each in moduleEvents:
            message += each.get('statusDesc') + '\n'
        
        return {
            'detect': True,
            'moduleEvents': moduleEvents,
            'message': message,
            'modulenos': modulenos
        }

    def _getAlarmIds(self, events):
        alarmIds = []
        for event in events:
            alarmId = event.get('alarmId')
            if alarmId is None:
                alarmId = '001233'
            alarmIds.append(alarmId)
        return list(set(alarmIds))
