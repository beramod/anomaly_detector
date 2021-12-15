import datetime
from src.detector.detectors import BaseDetector



## Inverter 검증
class BaseDetector30xx(BaseDetector):
    def _detectCommError(self, module, message):
        data = self._data

        option = self._eventSpec.get('option')
        timeDiffTreshold = option.get('timeDiffThreshold')
        timeDiffUnit = option.get('timeDiffUnit')

        timeDiff = self._checkCollectTimeDiff(data.get('datetimeAt'))
        if timeDiffUnit == 'min':
            timeDiff /= 60
        
        if timeDiffTreshold >= timeDiff:
            return False

        event = self._creatEvent(module, [], {
            'message': message
        })

        return event

    def _detectModuleCommError(self, module, moduleName, moduleNoName, message):
        data = self._data

        option = self._eventSpec.get('option')
        timeDiffTreshold = option.get('timeDiffThreshold')
        timeDiffUnit = option.get('timeDiffUnit')
        moduleNos = []

        for each in data.get(moduleName):
            timeDiff = self._checkCollectTimeDiff(each.get('updateTime'))
            if timeDiffUnit == 'min':
                timeDiff /= 60
            elif timeDiffUnit == 'hour':
                timeDiff /= 3600
            
            if timeDiffTreshold < timeDiff:
                moduleNos.append(each.get(moduleNoName))

        if not moduleNos:
            return False

        event = self._creatEvent(module, moduleNos, {
            'message': message
        })

        return event

    def _internalCommError(self, module, moduleName, moduleNoName, message):
        data = self._data

        option = self._eventSpec.get('option')
        timeDiffTreshold = option.get('timeDiffThreshold')
        timeDiffUnit = option.get('timeDiffUnit')
        moduleNos = []

        if data.get(moduleName) is not None:
            for each in data.get(moduleName):
                timeDiff = self._checkCollectTimeDiff(each.get('updateTime'))
                if timeDiffUnit == 'min':
                    timeDiff /= 60
                elif timeDiffUnit == 'hour':
                    timeDiff /= 3600

                if timeDiffTreshold < timeDiff:
                    moduleNos.append(each.get(moduleNoName))

        if not moduleNos:
            return False

        event = self._creatEvent(module, moduleNos, {
            'message': message
        })

        return event

    def _externalCommError(self, data):
        option = self._eventSpec.get('option')
        timeDiffTreshold = option.get('timeDiffThreshold')
        timeDiffUnit = option.get('timeDiffUnit')

        timeDiff = self._checkCollectTimeDiff(data.get('datetimeAt'))
        if timeDiffUnit == 'min':
            timeDiff /= 60
        elif timeDiffUnit == 'hour':
            timeDiff /= 3600

        if timeDiffTreshold >= timeDiff:
            return False

        return True

    def _serverRestart(self):
        try:
            res = self._ninesApi.get('/api/v1/shell/restart', {'project': 'ems_local_client', 'mcno' : self._powerStationInfo.get('mcno'), 'target': 'master'})
        except Exception as e:
            return False
        processCnt = res.get('result').get('processCnt')
        if processCnt < 2:
            return False
        return True
