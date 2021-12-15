import datetime
from src.detector.detectors.g4000 import BaseDetectorForTemp

## ROOM Fault Temp 
## 2021. 06. 03 안좌 임시적 경보발생으로 제작 -- 추후 변경할 수 있음 ( 임시로 pmsControl 사용 )
class Detector4008(BaseDetectorForTemp):
    _eventCode = 4008

    def _process(self):
        data = self._data
        bms = self._sharedData.get('bms', self._mcno)
        if not bms:
            return False
        
        pmsControl = self._sharedData.get('pmsControl', self._mcno)
        if not pmsControl:
            return False
        
        bmsTempMap = self._getBmsTempMap(bms)
        controlTempMap = self._getControlTempMap(pmsControl)
        if controlTempMap is None or len(controlTempMap) < 1:
            return False
        
        faultList = self._checkTemp(bmsTempMap, controlTempMap)
        event = self._getFaultEvent(faultList)
        if event is None:
            return False

        return event

    def _getBmsTempMap(self, bms):
        bmsMap = {}
        for bms in bms.get('bms'):
            bmsno = bms.get('bmsno')
            systemCellMaxTemp = bms.get('systemCellMaxTemp')
            systemCellMinTemp = bms.get('systemCellMinTemp')
            rackCellMaxVPos = bms.get('rackCellMaxVPos')
            rackCellMinVPos = bms.get('rackCellMinVPos')
            bmsMap[bmsno] ={
                'systemCellMaxTemp': systemCellMaxTemp,
                'systemCellMinTemp': systemCellMinTemp,
                'rackCellMaxVPos' : rackCellMaxVPos,
                'rackCellMinVPos' : rackCellMinVPos
            }
        return bmsMap

    def _getControlTempMap(self, pmsControl):
        controlMap = {}
        cellTempSettings = pmsControl.get('cellTempSettings')
        if cellTempSettings is None or len(cellTempSettings) < 1:
            return {}
        for control in cellTempSettings:
            bmsno = control.get('bmsno')
            fault = control.get('fault')
            if controlMap.get(bmsno) is None:
                controlMap[bmsno] = {}
            controlMap[bmsno]['fault'] = fault
        return controlMap

    def _checkTemp(self, bmsTempMap, controlTempMap):
        faultList = []
        if bmsTempMap is None or len(bmsTempMap) < 1 or controlTempMap is None or len(controlTempMap) < 1:
            return []

        for idx in bmsTempMap:
            maxTemp = bmsTempMap.get(idx).get('systemCellMaxTemp')
            maxPos = bmsTempMap.get(idx).get('rackCellMaxVPos')
            minTemp = bmsTempMap.get(idx).get('systemCellMinTemp')
            minPos = bmsTempMap.get(idx).get('rackCellMinVPos')
            maxControl = controlTempMap.get(idx).get('fault').get('upper')
            minControl = controlTempMap.get(idx).get('fault').get('lower')
            try :
                if int(maxTemp) >= int(maxControl):
                    faultList.append({
                        'bmsno': int(idx),
                        'fault': 'max',
                        'temp': maxTemp,
                        'pos': maxPos
                    })
                if int(minTemp) <= int(minControl):
                    faultList.append({
                        'bmsno': int(idx),
                        'fault': 'min',
                        'temp': minTemp,
                        'pos': minPos
                    })
            except Exception as e:
                print('error : {}'.format(e) )
                return []
        return faultList

    def _getFaultEvent(self, faultList):
        event = None
        if faultList is None or len(faultList) < 1 : 
            return None
        if len(faultList) > 0:
            bmsnos, message = self._makeEventInfo(faultList)
            event = self._creatEvent('bms', bmsnos, {'message': message})
        return event

    def _makeEventInfo(self, faultList):
        bmsnos = []
        message = ''

        for each in faultList:
            bmsno = each.get('bmsno')
            temp = each.get('temp')
            pos = each.get('pos')
            pos = '셀 위치 없음' if (pos is None) or (pos == '') else pos
            message += '[{}] 온도: {}℃, 셀위치: {} \n '.format(bmsno, temp, pos)
            bmsnos.append(bmsno)

        return bmsnos, message