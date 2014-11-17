__author__ = 'rmartins'
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol

from os import urandom
from itertools import islice, imap, repeat
import string

import datetime


class DetectRingdown(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    delta_f_threshold = 0.04
    delta_t_threshold = 20  # miliseconds. In order to be considered contiguous the difference between the timestamps
                            # of two ringdowns must be less than this value.
    create_windows_previous_pmu = '---'
    calc_diff_previous_pmu = '---'
    server_id = '***'
    dict_pmus = {18: 'UFPA', 27: 'UFMA', 36: 'UFJF', 45: 'UFAC', 54: 'UFAM', 63: 'UFBA', 72: 'UFRGS', 81: 'UNIFAP',
                 90: 'UNIFEI', 99: 'UNB', 108: 'COPPE', 117: 'UFC', 126: 'USP-SC', 135: 'UTFPR', 144: 'UFSC',
                 153: 'UNIR', 162: 'UFMT', 171: 'UNIPAMPA', 180: 'UFMG', 189: 'UFMS', 198: 'UFPE', 207: 'UFT'}

    def steps(self):
        return [MRStep(mapper=self.create_measure_windows,
                       reducer=self.calc_diff),
                MRStep(mapper=self.create_diff_windows,
                       reducer=self.detect_ringdown)]
                #MRStep(mapper=self.group_PMUs,
                #       reducer=self.group_severity)]

    def rand_string(self, length=6):
        chars = set(string.ascii_uppercase + string.digits)
        char_gen = (c for c in imap(urandom, repeat(1)) if c in chars)
        return ''.join(islice(char_gen, None, length))

    def get_window_tags_1(self, timestamp):
        '''
        Each point is the centre of a 5 point window. Considering that the average difference between timestamps is
        16 ms (1/60s), the timestamp to the first point of the window is the (rounded) given timestamp - 2 * 16. To
        the second point, given timestamp - 16. Likewise, to the fourth is given timestamp + 16, and to the fifth,
        given timestamp + 2 * 16.
        The miliseconds in the timestamps are rounded because the they are not exact multiples of 16.
        '''
        ms = float(timestamp[20:23])
        ms /= 16
        if ms - int(ms) < 0.6:  # This adjustment is necessary because numbers whose decimal part is equal to 0.5
            ms = int(ms) + 0.4  # are rounded up, thus leaving a "hole" in the tags. This forces the timestamp with
                                # decimal part equal to 0.5 to be rounded down.
        if ms >= 62:
            ms = 61
        rounded_ms = int(round(ms) * 16)
        date_timestamp = datetime.datetime(int(timestamp[0:4]),    # Year
                                           int(timestamp[5:7]),    # Month
                                           int(timestamp[8:10]),   # Day
                                           int(timestamp[11:13]),  # Hour
                                           int(timestamp[14:16]),  # Minute
                                           int(timestamp[17:19]),  # Second
                                           1000 * rounded_ms)      # Milisecond (* 1000 because the parameter receives
                                                                   #               microseconds - 1s/1.000.000)

        # Notice that for each tag calculated below adjustments may be necessary, because 1000 (ms) is not a multiple
        # of 16. The miliseconds in the tags around zero must be quantized again.
        #
        # 0 - 32 = 968, which is not a multiple of 16. Thus, 968 must become 960. Similarly,
        # 0 - 16 (and 16 - 32) = 984, which, again, is not a multiple of 16. Therefore, 984 becomes 976.
        #
        # Also,
        # 976 + 32 = 008, which is not a multiple of 16. Therefore, 008 becomes 016.
        #
        # Another issue is that the last quantized tag before the next second (with milisecond = 0) is 976.
        # No values in the data lead to the tag 992. Due to this peculiarity, another quantization is required:
        # 960 + 32 (and 976 + 16) = 992, which must then become the next second (with 0 miliseconds).

        # ___________________________________________________________________________________________________
        tag_1 = (date_timestamp - datetime.timedelta(0, 0, 0, 32))
        if tag_1.microsecond // 1000 == 968 or tag_1.microsecond // 1000 == 984:
            tag_1 = (tag_1 - datetime.timedelta(0, 0, 0, 8))
        tag_1_str = tag_1.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        # ___________________________________________________________________________________________________

        # ___________________________________________________________________________________________________
        tag_2 = (date_timestamp - datetime.timedelta(0, 0, 0, 16))
        if tag_2.microsecond // 1000 == 984:
            tag_2 = (tag_2 - datetime.timedelta(0, 0, 0, 8))
        tag_2_str = tag_2.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        # ___________________________________________________________________________________________________

        # ___________________________________________________________________________________________________
        tag_3 = date_timestamp
        tag_3_str = tag_3.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        # ___________________________________________________________________________________________________

        # ___________________________________________________________________________________________________
        tag_4 = (date_timestamp + datetime.timedelta(0, 0, 0, 16))
        if tag_4.microsecond // 1000 == 992:
            tag_4 = (tag_4 + datetime.timedelta(0, 0, 0, 8))
        tag_4_str = tag_4.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        # ___________________________________________________________________________________________________

        # ___________________________________________________________________________________________________
        tag_5 = (date_timestamp + datetime.timedelta(0, 0, 0, 32))
        if tag_5.microsecond // 1000 == 8 or tag_5.microsecond // 1000 == 992:
            tag_5 = (tag_5 + datetime.timedelta(0, 0, 0, 8))
        tag_5_str = tag_5.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        # ___________________________________________________________________________________________________

        tags = [tag_1_str, tag_2_str, tag_3_str, tag_4_str, tag_5_str]
        return tags

    def get_window_tags(self, timestamp):
        k = 0.96  # This factor (960 / 1000) "normalizes" the timestamps, mapping an interval of 1000 ms into an
                  # interval of 960 ms, which corresponds to 60 slices of 16 miliseconds. Remember that the highest
                  # milisecond value in the sampled data is 983.

        tag_1_str = tag_2_str = tag_3_str = tag_4_str = tag_5_str = ''
        ms = float(timestamp[20:23])
        normalized_ms = ms * k
        tag_3 = round(normalized_ms/16)
        tag_1 = tag_3 - 2
        tag_2 = tag_3 - 1
        tag_4 = tag_3 + 1
        tag_5 = tag_3 + 2

        date_timestamp = datetime.datetime(int(timestamp[0:4]),    # Year
                                           int(timestamp[5:7]),    # Month
                                           int(timestamp[8:10]),   # Day
                                           int(timestamp[11:13]),  # Hour
                                           int(timestamp[14:16]),  # Minute
                                           int(timestamp[17:19]))  # Second

        if tag_3 < 2 or tag_3 > 57:
            if tag_3 == 0:
                tag_5_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_5
                tag_4_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_4
                tag_3_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_3
                date_timestamp = (date_timestamp - datetime.timedelta(seconds=1))
                tag_2 = 58
                tag_1 = 59
                tag_2_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_2
                tag_1_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_1
            if tag_3 == 1:
                tag_5_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_5
                tag_4_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_4
                tag_3_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_3
                tag_2_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_2
                date_timestamp = (date_timestamp - datetime.timedelta(seconds=1))
                tag_1 = 59
                tag_1_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_1
            if tag_3 == 58:
                tag_1_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_1
                tag_2_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_2
                tag_3_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_3
                tag_4_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_4
                date_timestamp = (date_timestamp + datetime.timedelta(seconds=1))
                tag_5 = 0
                tag_5_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_5
            if tag_3 == 59:
                tag_1_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_1
                tag_2_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_2
                tag_3_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_3
                date_timestamp = (date_timestamp + datetime.timedelta(seconds=1))
                tag_4 = 0
                tag_5 = 1
                tag_4_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_4
                tag_5_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_5
        else:
            tag_1_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_1
            tag_2_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_2
            tag_3_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_3
            tag_4_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_4
            tag_5_str = date_timestamp.strftime('%Y-%m-%d %H:%M:%S') + '.' + '%02d' % tag_5

        tags_str = [tag_1_str, tag_2_str, tag_3_str, tag_4_str, tag_5_str]
        return tags_str

    def create_measure_windows(self, _, current_line):
        if current_line is not None:
            if current_line['Quality'] == 29:  # Quality == 'Good'
                windows_tags = self.get_window_tags(current_line['Time'])
                for current_tag in windows_tags:
                    yield (current_line['HistorianID'], current_tag),\
                          (current_line['Time'], current_line['Value'])

    def calc_diff(self, key, points):
        pmu = key[0]
        points_dict = {}
        timestamp_window = []
        value_window = []
        window_count = 0
        for current_point in points:
            window_count += 1
            current_value = float(current_point[1])
            points_dict[current_point[0]] = current_value
        if window_count == 5:
            for key in sorted(points_dict):
                timestamp_window.append(key)
                value_window.append(points_dict[key])
            diff = (- value_window[4] + 8 * value_window[3] - 8 * value_window[1] + value_window[0]) / 0.016
            yield None, [pmu, timestamp_window[2], value_window[2], diff]

    def create_diff_windows(self, _, diffs):
        diff_tags = self.get_window_tags(diffs[1])
        for curr_tag in diff_tags:
            yield [diffs[0], curr_tag], [diffs[1], diffs[2], diffs[3]]

    def detect_ringdown(self, key, values):
        pmu = key[0]
        timestamp_window = []
        value_dict = {}
        value_window = []
        diff_dict = {}
        diff_window = []
        window_count = 0
        previous_direction = ''
        for current_value in values:
            window_count += 1
            value_dict[current_value[0]] = float(current_value[1])
            diff_dict[current_value[0]] = float(current_value[2])
            #timestamp_window.append(str(current_value[0]))
            #value_window.append(float(current_value[1]))
            #diff_window.append(float(current_value[2]))
        if window_count == 5:
            for key in sorted(value_dict):
                timestamp_window.append(key)
                value_window.append(value_dict[key])
                diff_window.append(diff_dict[key])
            if abs(value_window[4] - value_window[0]) > self.delta_f_threshold:
                ringdown = True
                for index in range(5):
                    if index == 0:
                        if diff_window[index] < 0:
                            previous_direction = 'NEG'
                        else:
                            previous_direction = 'POS'
                    else:
                        if diff_window[index] < 0:
                            present_direction = 'NEG'
                        else:
                            present_direction = 'POS'
                        if present_direction != previous_direction:
                            ringdown = False
                            break
                        previous_direction = present_direction
                if ringdown:
                    # Returns the center of the diff window
                    yield self.dict_pmus[pmu], timestamp_window[2]

    def group_PMUs(self, PMU, timestamp):
        yield PMU, timestamp

    def group_severity(self, PMU, timestamps):
        timestamp_array = []
        nb_of_timestamps = 0
        nb_of_ringdowns = 1
        for curr_timestamp in sorted(timestamps):
            date_timestamp = datetime.datetime(int(curr_timestamp[0:4]),    # Year
                                               int(curr_timestamp[5:7]),    # Month
                                               int(curr_timestamp[8:10]),   # Day
                                               int(curr_timestamp[11:13]),  # Hour
                                               int(curr_timestamp[14:16]),  # Minute
                                               int(curr_timestamp[17:19]),  # Second
                                        1000 * int(curr_timestamp[20:]))    # Milisecond (* 1000 because the parameter
                                                                            # receives microseconds - 1s/1.000.000)
            timestamp_array.append(date_timestamp)
            nb_of_timestamps += 1
        for index in range(1, nb_of_timestamps - 1):
            delta_t = timestamp_array[index + 1] - timestamp_array[index]
            if delta_t.microseconds / 1000 > self.delta_t_threshold or index == nb_of_timestamps - 1:
                if index == nb_of_timestamps - 1:
                    if delta_t.microseconds / 1000 > self.delta_t_threshold:
                        nb_of_ringdowns += 1
                    else:
                        nb_of_ringdowns = 1
                severity_group = (nb_of_ringdowns / 5) + 1
                yield PMU, severity_group
                nb_of_ringdowns = 1
            else:
                nb_of_ringdowns += 1

    def calc_diff_old(self, key, points):
        pmu = key[0]
        timestamp_window = []
        value_window = []
        window_count = 0
        if self.server_id == '***':
            self.server_id = self.rand_string()
        for current_point in points:
            window_count += 1
            timestamp_window.append(current_point[0])
            current_value = float(current_point[1])
            value_window.append(current_value)
        if window_count == 5:
            diff = (- value_window[4] + 8 * value_window[3] - 8 * value_window[1] + value_window[0]) / 0.016
            windows_tags = self.get_window_tags(key[1])
            for current_tag in windows_tags:
                yield [pmu, str(current_tag)[:-3]], [timestamp_window[2], value_window[2], diff,
                                                     self.server_id, str(datetime.datetime.now())]


if __name__ == '__main__':
    DetectRingdown.run()