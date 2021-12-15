import datetime
from src.detector.detectors import BaseDetector


## PCS 검증 ( 미방전 )
class Detector4003(BaseDetector):
    _eventCode = 4003

    def _process(self):
        pcsnos = [1]
        soc = self._getBmsSoc()
        if soc is None:
            return False
        isDischarge = self._isPcsDischargeMode()
        if isDischarge is None:
            return False
        isSchedule = self._isDischargeSchedule()
        if not isSchedule:
            return False
        socLowerLimit = self._getPcsSocLowerLimit()
        socThreshold = socLowerLimit * 2
        if (soc <= socThreshold) or isDischarge:
            return False

        event = self._creatEvent('pcs', pcsnos, {
            'message': '방전시간내 미방전. 현재 soc: {0:.1f} 하한 soc: {1:.1f}/ '.format(soc, socLowerLimit)
        })

        return event

    def _getBmsSoc(self) -> float:
        bms = self._sharedData.get('bms', self._mcno)

        if not bms:
            return None
        soc = 100
        for bmsObj in bms.get('bms'):
            soc = min(soc, bmsObj.get('systemSOC'))
        return soc

    def _getPcsSocLowerLimit(self) -> float:
        pms = self._sharedData.get('pms', self._mcno)
        if not pms:
            return None
        socLowerLimit = 0
        for pmsObj in pms.get('pms'):
            socLowerLimit = pmsObj.get('pcsSOCLowerLimit') or 0
        return round(socLowerLimit, 2)

    def _isPcsDischargeMode(self) -> bool:
        pcs = self._sharedData.get('pcs', self._mcno)
        if not pcs:
            return None
        isDischarge = False
        for pcsObj in pcs.get('pcs'):
            tmpDischarge = pcsObj.get('statusDischarge') or False
            if tmpDischarge:
                isDischarge = True
        return isDischarge

    def _isDischargeSchedule(self) -> bool:
        pms = self._sharedData.get('pms', self._mcno)
        if not pms:
            return None
        isSchedule = False
        nowTime = int(self._timestamp.strftime('%H%M'))
        for pmsObj in pms.get('pms'):
            schedules = pmsObj.get('schedules')
            for schedule in schedules:
                action = schedule.get('action')
                if action == 'charge':
                    continue
                startTime, endTime = schedule.get('startTime'), schedule.get('endTime')
                if int(startTime) <= nowTime <= int(endTime):
                    isSchedule = True
        return isSchedule
