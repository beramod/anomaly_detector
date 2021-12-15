import datetime
import uuid
from src.detector.detectors.g3000 import BaseDetector30xx


# 외부 통신에러
class Detector3010(BaseDetector30xx):
    _eventCode = 3010

    def _process(self):
        targets = self._getTargetModule()
        checkEvent = []
        for target in targets:
            checkEvent.append(self._externalCommError(self._sharedData.get(target, self._mcno)))

        if len(checkEvent) == 0:
            return False

        if checkEvent.count(True) != len(checkEvent):
            return False

        event = self._creatEvent('', [], {
            'message': '외부통신에러'
        })

        self._serverRestart() # 응답받을경우 10초이상 소요되므로 재시작 명령어만 내리는 방향으로

        return event

    def _getTargetModule(self):
        targets = []
        modules = self._powerStationInfo.get('modules')

        if modules is None:
            return targets

        for module in modules:
            key = module
            if key == 'envSensorEms':
                key = 'emsEnv'
            docs = self._sharedData.get(key, self._mcno)
            if docs:
                targets.append(key)

        return targets
