import datetime
from src.define import *
from src.detector.detectors import BaseDetector


## PCS 검증 ( 미충전 )
class Detector4002(BaseDetector):
    _eventCode = 4002

    def _process(self):
        data = self._data
        pcsnos = [1]

        pcsCharge = data.get('pcs')[0].get('statusCharge')
        activeKw = self._getActiveKw()
        soc = self._getBmsSoc()
        socUpper = self._getSocUpper()
        socThreshold = socUpper * 0.95
        minActiveKw = self._getMinActiveKw()
        
        isSchedule = self._isChargeSchedule()
        if not isSchedule:
            return False

        if activeKw == None or soc == None:
            return False

        if (soc >= socThreshold) or (activeKw < minActiveKw) or pcsCharge:
            return False

        event = self._creatEvent('pcs', pcsnos, {
            'message': '충전시간내 미충전 / active kW: {0:.1f}, soc: {1:.1f}, soc상한: {2:.1f}'.format(activeKw, soc, socUpper)
        })

        return event

    def _getActiveKw(self):
        meter = self._sharedData.get('meter', self._mcno)
        if meter:
            activeKw = pqm = acb = vcb = 0
            checkPQM = checkACB = checkVCB = False
            for each in meter.get('meters'):
                if each.get('device') == 'PV':
                    if each.get('module') == 'PQM':
                        pqm += each.get('activeKw')
                        checkPQM = True
                    if each.get('module') == 'ACB':
                        acb += each.get('activeKw')
                        checkACB = True
                    if each.get('module') == 'VCB':
                        vcb += each.get('activeKw')
                        checkVCB = True
            activeKw = pqm if checkPQM else acb if checkACB else vcb if checkVCB else 0
            if activeKw != 0:
                return activeKw
        inverter = self._sharedData.get('inverter', self._mcno)
        ackw = 0
        if inverter:
            for each in inverter.get('inverters'):
                ackw += each.get('ackw')
        if not meter and not inverter:
            return None
        return ackw

    def _getBmsSoc(self):
        bms = self._sharedData.get('bms', self._mcno)
        if not bms:
            return None
        soc = 0
        for each in bms.get('bms'):
            soc += each.get('systemSOC')
        soc /= len(bms.get('bms'))
        return soc

    def _getSocUpper(self):
        pms = self._sharedData.get('pms', self._mcno)
        socUpper = 80
        if pms:
            if pms.get('pms') and pms.get('pms')[0]:
                if pms.get('pms')[0].get('pcsSOCUpperLimit'):
                    socUpper = pms.get('pms')[0].get('pcsSOCUpperLimit')
        return socUpper
    
    def _getMinActiveKw(self):
        pms = self._sharedData.get('pms', self._mcno)
        minActiveKw = MIN_ACTIVE_KW_FOR_CHARGE
        if pms:
            if pms.get('pms') and pms.get('pms')[0]:
                if pms.get('pms')[0].get('minActiveKw'):
                    minActiveKw = pms.get('pms')[0].get('minActiveKw')
        return minActiveKw

    def _isChargeSchedule(self) -> bool:
        pms = self._sharedData.get('pms', self._mcno)
        if not pms:
            return None
        isSchedule = False
        nowTime = int(self._timestamp.strftime('%H%M'))
        for pmsObj in pms.get('pms'):
            schedules = pmsObj.get('schedules')
            for schedule in schedules:
                action = schedule.get('action')
                if action == 'discharge':
                    continue
                startTime, endTime = schedule.get('startTime'), schedule.get('endTime')
                if int(startTime) <= nowTime <= int(endTime):
                    isSchedule = True
        return isSchedule
