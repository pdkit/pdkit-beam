from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.transforms import window
import numpy as np
import pandas as pd
import time
import datetime
import pdkit
import warnings
warnings.filterwarnings("ignore")


class TransformTimestampDoFn(beam.DoFn):
    def process(self, element):
        measurement_time = element.split(',')[0]
        yield beam.window.TimestampedValue(element, int(measurement_time)/1000000000.0)


class ParseAccEventFn(beam.DoFn):
    """Parses the raw acceleration data event info into a Python dictionary.

    Each event line has the following format:
    timestamp_in_microseconds,x,y,z,username
    """
    def __init__(self):
        super(ParseAccEventFn, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            row = list(csv.reader([elem]))[0]
            yield {
                'user':int(row[4]),
                'timestamp': int(row[0])/1000000000.0,
                'x': float(row[1]),
                'y': float(row[2]),
                'z': float(row[3]),
                'mag_sum_acc': float(np.sqrt(float(row[1]) ** 2 + float(row[2]) ** 2 + float(row[3]) ** 2))
            }
        except:
            # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)


class ExtractDoFn(beam.DoFn):
    """Parse each line of input text into words."""
    def __init__(self):
        super(ExtractDoFn, self).__init__()

    def process(self, element, *args, **kwargs):
        yield element['user'],(element['timestamp'],element['mag_sum_acc'])


class CalculatePDkitMethodDo(beam.DoFn):
    def __init__(self):
        super(CalculatePDkitMethodDo, self).__init__()

    def process(self, element, *args, **kwargs):
        user, mag_sum_accs = element

        if user and mag_sum_accs:
            ind, vals = zip(*mag_sum_accs)
            ser1 = pd.Series([float(v) for v in vals],
                             index=[pd.Timestamp(i) for i in ind])
            tp = pdkit.TremorProcessor()
            yield user, tp.spkt_welch_density(ser1)[0][1]
        else:
            yield user, 0


class CalculatePDkitMethod(beam.PTransform):
    def __init__(self):
        super(CalculatePDkitMethod, self).__init__()

    def expand(self, pcoll):
        return (
            pcoll
            | 'abs energy map' >> beam.ParDo(CalculatePDkitMethodDo())
        )


class UserDict(beam.DoFn):

    def process(self, team_score, window=beam.DoFn.WindowParam):
        ts_format = '%H:%M:%S'
        user, result = team_score
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        yield {
            'user': user,
            'result': result,
            'start': window_start,
            'end': window_end
}


class ParseMagSumAcc(beam.PTransform):
    def __init__(self, window_duration, window_overlap):
        super(ParseMagSumAcc, self).__init__()
        self.window_duration = window_duration
        self.window_overlap = window_overlap

    def expand(self, pcoll):
        return (
            pcoll
            | 'Timestamp' >> beam.ParDo(TransformTimestampDoFn())
            | 'Window' >> beam.WindowInto(window.SlidingWindows(self.window_duration, self.window_overlap))
            | 'ParseAccEventFn' >> beam.ParDo(ParseAccEventFn())
            | 'Extract:' >> beam.ParDo(ExtractDoFn())
        )


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='tremor_data_with_user.csv',
                        help='Input file to process.')
    parser.add_argument('--input_topic',
                        help='Input PubSub topic of the form "projects/<PROJECT>/topics/<TOPIC>".')
    parser.add_argument('--output_topic',
                        help='Output PubSub topic of the form "projects/<PROJECT>/topics/<TOPIC>".')
    parser.add_argument('--output',
                        dest='output',
                        default='output.csv',
                        help='output file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    if known_args.input_topic:
        options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Read from PubSub into a PCollection.
        if known_args.input_topic:
            messages = (p
                        | 'Read from Stream' >> beam.io.gcp.pubsub.ReadFromPubSub(topic=known_args.input_topic)
                        .with_output_types(bytes))

        else:
            messages = (p
                        | 'Read from file' >> ReadFromText(known_args.input))

        windowed_data = (messages
                         | 'ParseMagSumAcc' >> ParseMagSumAcc(30,10)
                         )

        grouped = (windowed_data
                   | 'GroupWindowsByUser' >> beam.GroupByKey()
                   | 'Calculate pdkit method' >> CalculatePDkitMethod()
                   | 'UserScoresDict' >> beam.ParDo(UserDict())
                   )

        welch = (grouped
                 | 'Parse it' >> beam.Map(lambda elem: (elem['user'], elem['result'], elem['start'], elem['end']))
                 )

        # Format the counts into a PCollection of strings.
        def format_result(element):
            (user, result, start, end) = element
            return '%d,%f,%s,%s' % (user, result, start, end)

        output = welch | 'format' >> beam.Map(format_result)

        if known_args.output_topic:
            output | 'stream output' >> beam.io.gcp.pubsub.WriteStringsToPubSub(known_args.output_topic)
        else:
            output | 'write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()