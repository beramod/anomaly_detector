import datetime
from src.detector.detectors.g4000 import BaseDetectorForTemp


## PCS ROOM Warning Temp
class Detector4010(BaseDetectorForTemp):
    _eventCode = 4010

    def _process(self):
        data = self._data
        if data is None or data.get('tempHumiditySensors') is None or len(data.get('tempHumiditySensors')) < 1:
            return False

        pcsRoomTempSettings = self._getRoomTempSettings('PCS')
        if pcsRoomTempSettings is None or len(pcsRoomTempSettings) < 1:
            return False

        faultTempSettingMap = self._getRoomFaultSettingsMap(pcsRoomTempSettings)
        if faultTempSettingMap is None or faultTempSettingMap == {}:
            return False

        # None or all(전체 장비에 동일 기준 적용) or units(장비별 개별 적용)
        mode = self._getFaultCheckMode(faultTempSettingMap)
        if mode is None:
            return False

        event = self._getWarningEvent(data, faultTempSettingMap, mode)

        if event is None:
            return False

        return event

    def _getWarningEvent(self, data, warningSettingMap, mode):
        event = None
        faultInfos = []

        for each in data.get('tempHumiditySensors'):
            if each.get('room') != 'PCS':
                continue
            sensorno = int(each.get('sensorno'))
            temp = each.get('temp')
            tempLimitMap = warningSettingMap.get(0).get('warning') if mode == 'all' else warningSettingMap.get(
                sensorno).get('warning')

            if temp >= tempLimitMap.get('upper'):
                info = {
                    'sensorno': sensorno,
                    'faultTemp': temp
                }
                faultInfos.append(info)

        if len(faultInfos) > 0:
            sensornos, message = self._makeEventInfo(faultInfos)
            event = self._creatEvent('emsEnv', sensornos, {'message': message})

        return event

    def _makeEventInfo(self, faultInfos):
        sensornos = []
        message = 'PCS 룸 온도 경고'

        for each in faultInfos:
            sensorno = int(each.get('sensorno'))
            faultTemp = each.get('faultTemp')
            message = '{0} \ 모듈 번호: {1} 온도: {2:.1f}'.format(message, str(sensorno), faultTemp)
            sensornos.append(sensorno)

        return sensornos, message