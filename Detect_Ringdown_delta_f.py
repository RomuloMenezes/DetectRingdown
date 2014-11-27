__author__ = 'rmartins'
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
from operator import itemgetter
import datetime


class DetectRingdown(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    delta_f_threshold = 0.5
    delta_t_threshold = 3  # The severity is calculated for intervals of this size (in seconds)
    delta_t_vicinity_limit = 20  # miliseconds. In order to be considered contiguous the difference between the
                                 # timestamps of two ringdowns must be less than this value.
    dict_pmus = {18: 'UFPA', 27: 'UFMA', 36: 'UFJF', 45: 'UFAC', 54: 'UFAM', 63: 'UFBA', 72: 'UFRGS', 81: 'UNIFAP',
                 90: 'UNIFEI', 99: 'UNB', 108: 'COPPE', 117: 'UFC', 126: 'USP-SC', 135: 'UTFPR', 144: 'UFSC',
                 153: 'UNIR', 162: 'UFMT', 171: 'UNIPAMPA', 180: 'UFMG', 189: 'UFMS', 198: 'UFPE', 207: 'UFT'}

    def mapper(self, _, current_line):
        if current_line is not None:
            if current_line['Quality'] == 29:  # Quality == 'Good'
                if float(current_line['Value']) < 60 - self.delta_f_threshold:
                    yield current_line['HistorianID'], (current_line['Time'], current_line['Value'])

    def reducer(self, pmu, points):
        points_list = []
        number_of_points = 1
        severity = 0
        for current_point in points:
            points_list.append(current_point)
        previous_timestamp = window_start_timestamp = self.string_to_datetime(points_list[0][0])
        for current_point in sorted(points_list, key=itemgetter(0)):
            current_timestamp = self.string_to_datetime(current_point[0])
            delta_t = abs(current_timestamp - previous_timestamp)
            if delta_t.microseconds / 1000 <= self.delta_t_vicinity_limit:
                number_of_points += 1
            else:
                severity = number_of_points / (self.delta_t_threshold * 60)
                if severity > 0:
                    print(self.dict_pmus[pmu], [str(window_start_timestamp), severity])
                    severity = 0
                number_of_points = 1
                window_start_timestamp = current_timestamp
            previous_timestamp = current_timestamp
        # The following if treats the case in which the data chunk ends before reaching the "stop counting and print"
        # condition.
        if delta_t.microseconds / 1000 <= self.delta_t_vicinity_limit:
            severity = number_of_points / (self.delta_t_threshold * 60)
            if severity > 0:
                print(self.dict_pmus[pmu], [str(window_start_timestamp), severity])

    def string_to_datetime(self, string):
        date_timestamp = datetime.datetime(int(string[0:4]),    # Year
                                           int(string[5:7]),    # Month
                                           int(string[8:10]),   # Day
                                           int(string[11:13]),  # Hour
                                           int(string[14:16]),  # Minute
                                           int(string[17:19]),  # Second
                                    1000 * int(string[20:23]))  # Milisecond (* 1000 because the parameter receives
                                                                #               microseconds - 1s/1.000.000)
        return date_timestamp


if __name__ == '__main__':
    DetectRingdown.run()