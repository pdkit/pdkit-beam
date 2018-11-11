from __future__ import print_function
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import ReadFromText

import argparse
import logging
from apache_beam.transforms import window
import pandas as pd
import numpy as np
import datetime
import time


class TransformTimestampDoFn(beam.DoFn):
    def process(self, element):
        measurement_time = element.split(',')[0]
        # t = time.localtime(epoch / 1000000)
        # unix_timestamp = time.mktime(datetime.datetime.strptime(measurement_time, '%H:%M:%S.f').timetuple())
        # unix_timestamp = int(datetime.datetime.strptime(measurement_time, '%H:%M:%S').strftime("%s"))
        yield beam.window.TimestampedValue(element, int(measurement_time)/1000000000.0)
        # yield beam.window.TimestampedValue(element, unix_timestamp)


class FormatDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        ts_format = '%H:%M:%S.%f UTC'
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        return [{'count': element,
                 'window_start':window_start,
                 'window_end':window_end}]


class MagSumAcc(beam.DoFn):
    """Parse each line of input text into mag_sum_acc"""

    def __init__(self):
        super(MagSumAcc, self).__init__()

    def process(self, element, *args, **kwargs):
        x = element.split(',')[1]
        y = element.split(',')[2]
        z = element.split(',')[3]

        yield np.sqrt(float(x) ** 2 + float(y) ** 2 + float(z) ** 2)


def b_mean(values):
    # ser1 = pd.Series([float(v) for v in values],
    #                  index=[pd.Timestamp(i) for i in ind])
    # return ser1.mean
    return np.mean(values)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                      dest='input',
                      default='short_tremor_data.csv',
                      help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=PipelineOptions()) as p:
        acceleration_data = (p
                             | 'Read' >> ReadFromText(known_args.input)
                             # | 'Timestamp' >> beam.Map(lambda x: beam.window.TimestampedValue( x, extract_timestamp(x)))
                             | 'Timestamp' >> beam.ParDo(TransformTimestampDoFn())
                             )

        windowed_data = (
                acceleration_data
                | 'Window' >> beam.WindowInto(window.SlidingWindows(30,10))
                # | 'Count' >> (beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults())
                | 'Clean' >>beam.ParDo(MagSumAcc())
               # | 'Format' >> beam.ParDo(FormatDoFn())
        )

        added_value = (windowed_data
                 | 'Count' >> (beam.CombineGlobally(b_mean).without_defaults())
                 )

        # sums | 'Print' >> beam.ParDo(lambda (x): print('%s' % (x)))
        added_value | beam.ParDo(lambda (x): print(x))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()