from src.detector.detectors.g4000 import BaseDetector

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
                'message': 'test'
            })
            return event
        return False

    ## 임시코드
    def _diff_output_current(self):
        data = self._data
        combiner_boxes = data.get('combinerBoxes')
        diffs = []
        current_diff_threshold = 6 ## 12 / 2
        for combiner_box_obj in combiner_boxes:
            currents = list(map(lambda el: el.get('inputA'), combiner_box_obj.get('channels')))
            filtered_currents = list(filter(lambda el: el != 0, currents))
            if len(filtered_currents) == 0:
                continue
            current_avg = sum(filtered_currents) / len(filtered_currents)
            current_min = min(currents)
            current_max = max(currents)
            diff = (current_max - current_min) > current_diff_threshold
            if diff:
                diffs.append(combiner_box_obj.get('combinerboxno'))
        if len(diffs) > 0:
            event = self._creatEvent('combinerBox', diffs, {
                'message': 'test'
            })
            return event
        return False
