import datetime
from src.detector.detectors import BaseDetector


# Temp 체크 용 Base Detector
class BaseDetectorForTemp(BaseDetector):
    def _getRoomTempSettings(self, room):
        pmsControl = self._sharedData.get('pmsControl', self._mcno)
        if pmsControl is None:
            return None
        
        controlSetting = pmsControl.get('controlSetting')
        if controlSetting is None:
            return None
        
        tempSettings = controlSetting.get('tempSettings')
        if tempSettings is None or type(tempSettings) != list:
            return None
        
        batTempSettings = None
        for each in tempSettings:
            if each.get('room') != room:
                continue
            if batTempSettings is None:
                batTempSettings = []
            batTempSettings.append(each)

        return batTempSettings

    def _getRoomFaultSettingsMap(self, tempSettings):
        if tempSettings is None:
            return None
        faultTempSetting = {}

        for each in tempSettings:
            sensorno = int(each.get('sensorno'))
            faultTempSetting[sensorno] = each

        return faultTempSetting

    def _getFaultCheckMode(self, faultSettingMap):
        mode = None
        sensornos = faultSettingMap.keys()

        if len(sensornos) < 1:
            return None

        if 0 in sensornos:
            mode = 'all'
        else:
            mode = 'units'
        
        return mode