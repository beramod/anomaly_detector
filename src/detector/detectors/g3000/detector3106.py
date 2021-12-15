import datetime
from src.detector.detectors.g3000 import BaseDetector30xx


## EMS ENV 외부 통신에러
class Detector3106(BaseDetector30xx):
    _eventCode = 3106

    def _process(self):
        emsEnvList = ['aircons', 'tempHumiditySensors', 'trSensors', 'doorSensors', 'fireSensors', 'ups' ]
        emsEnvNoList = ['airconno', 'sensorno', 'trno', 'doorno', 'fireno', 'upsno']

        event = self._internalCommError('emsEnv', emsEnvList, emsEnvNoList, '')
        return event


    # Override
    def _internalCommError(self, module, moduleName, moduleNoName, message):
        data = self._data
        moduleNos = {}
        option = self._eventSpec.get('option')
        timeDiffTreshold = option.get('timeDiffThreshold')
        timeDiffUnit = option.get('timeDiffUnit')

        for name in moduleName:
            moduleNos[name] = []

        for idx in range(len(moduleName)):
            if data.get(moduleName[idx]) is not None and (len(moduleName) == len(moduleNoName)):
                for each in data.get(moduleName[idx]):
                    if each.get('updateTime') is None:
                        continue
                    timeDiff = self._checkCollectTimeDiff(each.get('updateTime'))
                    if timeDiffUnit == 'min':
                        timeDiff /= 60
                    elif timeDiffUnit == 'hour':
                        timeDiff /= 3600

                    if timeDiffTreshold < timeDiff:
                        moduleNos[moduleName[idx]].append(each.get(moduleNoName[idx]))
    
        commErrorCnt = 0
        for module in moduleNos:
            if len(moduleNos[module]) > 0:
                commErrorCnt += len(moduleNos[module])
        
        if commErrorCnt == 0:
            return False

        event = self._creatEvent(module, moduleNos, {
            'message' : message
        })
        return event