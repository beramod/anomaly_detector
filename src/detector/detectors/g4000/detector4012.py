from src.detector.detectors import BaseDetector

class Detector4012(BaseDetector):
    _eventCode = 4012

    def _process(self):
        data = self._data
        meta_info = self._sharedData.get('combinerBoxMeta', self._mcno)
        combs = data.get('combinerBoxes')
        pre_data = self._sharedData.get('combinerBox', self._mcno)
        pre_combs = pre_data.get('combinerBoxes')
        max_potential = meta_info.get('maxPotential')
        diff_map = {}

        for comb_obj in pre_combs:
            diff_map[comb_obj.get('combinerboxno')] = comb_obj.get('outputV')

        for comb_obj in combs:
            pre_val = diff_map.get(comb_obj.get('combinerboxno'))
            if pre_val == 0:
                diff_map[comb_obj.get('combinerboxno')] = 0
                continue
            diff_val = (abs(pre_val - comb_obj.get('outputV')) / max_potential) * 100
            diff_map[comb_obj.get('combinerboxno')] = diff_val

        module_nos = []

        for no in diff_map:
            if diff_map.get(no) > 50:
                module_nos.append(no)

        if len(module_nos) > 0:
            event = self._creatEvent('combinerBox', module_nos, {
                'message': '접속반 전압 급감. 화재 발생 확인 필요.'
            })
            return event
        return False