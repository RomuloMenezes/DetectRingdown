#!/usr/bin/env python

import datetime
import sys
import json

def create_measure_windows():
    for current_line in sys.stdin:
        if current_line is not None:
            historian_id, time, value, quality = json.loads(current_line)
            if quality == 29:  # Quality == 'Good'
                windows_tags = self.get_window_tags(time)
                for current_tag in windows_tags:
                    yield (historian_id, current_tag), (time, value)

def get_window_tags(timestamp):
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


if __name__ == "__main__":
    create_measure_windows()