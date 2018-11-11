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
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.transforms import window
import numpy as np
import pandas as pd
import time
import datetime
import pdkit


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


class MyMeanCombineFn(beam.CombineFn):
    def create_accumulator(self):
        """Create a "local" accumulator to track sum and count."""
        return (0, 0)
    def add_input(self, (sum_, count), element):
        """Process the incoming value."""
        return sum_ + element, count + 1
    def merge_accumulators(self, accumulators):
        """Merge several accumulators into a single one."""
        sums, counts = zip(*accumulators)
        return sum(sums), sum(counts)
    def extract_output(self, (sum_, count)):
        """Compute the mean average."""
        if count == 0:
            return float('NaN')
        return sum_ / float(count)


# [START extract_and_sum_score]
class ExtractAndMeanMagSumAcc(beam.PTransform):
    """A transform to extract key/mag_sum_acc information and means the mag_sum_acc.
    The constructor argument `field` determines that 'user' info is extracted.
    """
    def __init__(self, field):
        super(ExtractAndMeanMagSumAcc, self).__init__()
        self.field = field

    def expand(self, pcoll):
        return (pcoll
                | beam.Map(lambda elem: (elem[self.field], elem['mag_sum_acc']))
                | beam.CombinePerKey(MyMeanCombineFn())
                )
# [END extract_and_sum_score]


class ExtractDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def __init__(self):
        super(ExtractDoFn, self).__init__()

    def process(self, element, *args, **kwargs):
        yield element['user'],(element['timestamp'],element['mag_sum_acc'])


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
            # Extract username/mag_sum_acc pairs from the event data.
            # | 'ExtractAndSumScore' >> ExtractAndMeanMagSumAcc('user')
            | 'Extract:' >> beam.ParDo(ExtractDoFn())
        )


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
            yield user, tp.number_peaks(ser1)
        else:
            yield user, 0


def CalculatePandasAbsoluteEnergyMap(element):
    user, mag_sum_accs = element

    if user and mag_sum_accs:
        ind, vals = zip(*mag_sum_accs)
        ser1 = pd.Series([float(v) for v in vals],
                         index=[pd.Timestamp(i) for i in ind])
        tp = pdkit.TremorProcessor()
        return user, tp.abs_energy(ser1)
    else:
        return user, 0


class CalculatePDkitMethod(beam.PTransform):
    def __init__(self):
        super(CalculatePDkitMethod, self).__init__()
    def expand(self, pcoll):
        return (
            pcoll
            | 'abs energy map' >> beam.ParDo(CalculatePDkitMethodDo())
            # | 'abs energy map' >> beam.Map(CalculatePandasAbsoluteEnergyMap)
        )

# [START extract_and_sum_score]
class ExtractAndSumScore(beam.PTransform):
    """A transform to extract key/score information and sum the scores.
    The constructor argument `field` determines whether 'team' or 'user' info is
    extracted.
    """
    def __init__(self, field):
        super(ExtractAndSumScore, self).__init__()
        self.field = field

    def expand(self, pcoll):
        return (pcoll
                | beam.Map(lambda elem: (elem[self.field], elem['abs_a']))
                | beam.CombinePerKey(sum)
                )
# [END extract_and_sum_score]


class ParseFinalDataDoFn(beam.DoFn):
    def __init__(self):
        super(ParseFinalDataDoFn, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            # yield {
            #     'user': int(user),
            #     'msa': float(msa),
            # }
            return [{'user':int(elem[0]), 'msa':float(elem[1])}]
        except:
            # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)


# class FormatDoFn(beam.DoFn):
#     def process(self, element):
#         return [{
#             'user': element[0],
#             'msa': element[1]
#         }]

class FormatDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        ts_format = '%H:%M:%S.%f UTC'
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        return [{'user': int(element[0]),
                 'abs_e': float(element[1]),
                 'window_start':window_start,
                 'window_end':window_end}]


class UserDict(beam.DoFn):
    def process(self, team_score, window=beam.DoFn.WindowParam):
        ts_format = '%H:%M:%S.%f UTC'
        user, result = team_score
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        yield {
            'user': user,
            'result': result,
            'start': window_start,
            'end': window_end
}


class ParsePDkitData(beam.DoFn):
    def __init__(self):
        super(ParsePDkitData, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        # try:
        row = list(csv.reader([elem]))[0]
        yield int(row[0]), float(row[1])
        # yield {
        #     'user': int(row[0]),
        #     'abs_energy': float(row[1])
        # }
        # except:
            # pylint: disable=bare-except
            # Log and count parse errors
            # self.num_parse_errors.inc()
            # logging.error('Parse error on "%s"', elem)



# [START main]
def run(argv=None):
    """Main entry point; defines and runs the PDkit_score pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='tremor_data_with_user.csv',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        default='absolute_energy.csv',
                        help='output file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        windowed_data = (p
                         | 'Read' >> ReadFromText(known_args.input)
                         | 'ParseMagSumAcc' >> ParseMagSumAcc(30,10)
                         # | 'GroupWindowsByUser' >> beam.GroupByKey()
                         # | 'Format' >> beam.ParDo(FormatDoFn())
                         # | 'Calculate pdkit absolute energy' >> beam.Map(CalculatePandasAbsoluteEnergyMap)
                         # | 'Calculate pdkit absolute energy' >> beam.ParDo(CalculatePandasAbsoluteEnergyDo())
                         # | 'GroupByUserAgain' >> beam.GroupByKey()
                         # | 'Mean it' >> beam.CombineValues(beam.combiners.MeanCombineFn())
                         )

        # pdkit_data = (windowed_data
                      # | 'ExtractAndSumScore' >> ExtractAndMeanMagSumAcc('user')
                      # | 'ExtractAndSumScore' >> beam.Map(lambda elem: ('user', elem['mag_sum_acc']))
                      # | 'ExtractAndSumScore:' >> beam.ParDo(ParseUserMSADoFn())
                      # )

        # group mag_sum_acc by user key and windowed data
        grouped = ( windowed_data
                    # | 'GroupByUser' >> beam.GroupByKey()
                  | 'GroupWindowsByUser' >> beam.GroupByKey()
                  | 'Calculate pdkit method' >> CalculatePDkitMethod()
                    # | 'Calculate pdkit absolute energy' >> beam.ParDo(CalculatePandasAbsoluteEnergyDo())
                  # | 'Calculate pdkit absolute energy' >> beam.Map(CalculatePandasAbsoluteEnergyMap)
                  # | 'Format' >> beam.ParDo(FormatDoFn())
                  #   | 'GroupByUserAgain' >> beam.CoGroupByKey()
                    # | 'Mean it' >> beam.CombineValues(beam.combiners.MeanCombineFn())
                    # | 'format_user_data' >> beam.ParDo(ParseFinalDataDoFn())
                    # | 'ExtractAndSumScore' >> ExtractAndSumScore('user')
                    # | 'Format' >> beam.ParDo(FormatDoFn())
                    # | 'GroupAndSum' >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
                    | 'UserScoresDict' >> beam.ParDo(UserDict())
                  )

        pdkit_method_data = (grouped
                 | 'Parse it' >> beam.Map(lambda elem: (elem['user'], elem['result']))
                 # | 'GroupAndMean' >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
                 )

        # abs_e = (grouped
        #          | 'Calculate pdkit absolute energy' >> beam.ParDo(CalculatePandasAbsoluteEnergy())
        #          | 'format_user_data' >> beam.ParDo(ParseFinalDataDoFn())

                 # | 'da' > beam.CombineGlobally(beam.combiners.MeanCombineFn()).without_defaults()
                 # | 'GroupAndSum' >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
                 # | 'ExtractAndSumScore' >> ExtractAndSumScore('user')
                 # | 'mean' >> beam.CombinePerKey(MyMeanCombineFn())
                 # )
        # combined = (abs_e
                    # | 'mean combine' >> beam.CombinePerKey(sum)
                    # | 'format_user_data' >> beam.Map(format_user_data)
                    # | 'ExtractAndSumScore' >> ExtractAndSumScore('user')
        # )

        # print(abs_e)
        # abs_e | beam.ParDo(lambda (x): print(x))
        # windowed_data | beam.ParDo(lambda (x): print(x))

        # Format the counts into a PCollection of strings.
        def format_result(element):
            (user, result) = element
            return '%d,%f' % (user, result)

        output = pdkit_method_data | 'format' >> beam.Map(format_result)

        output | 'write' >> WriteToText(known_args.output)

        # abs_e | beam.ParDo(lambda (x): print(x))
        # abs_energy_list = (abs_e | 'Combine' >> beam.CombineGlobally(beam.combiners.ToDict()).without_defaults())
        # abs_energy_list | beam.ParDo(lambda (x): print(x))
        #
        # abs_energy_list | beam.ParDo(lambda (x): print(x))


    with beam.Pipeline(options=options) as q:
        # Format the counts into a PCollection of strings.
        def format_result(element):
            (user, result) = element
            return 'user_id(%d), value:%f' % (user, result)
        abs_energy_windowed = (q
                               | 'Read PDkit data' >> ReadFromText('absolute_energy.csv-00000-of-00001')
                               | 'ParsePDkitData' >> beam.ParDo(ParsePDkitData())
                               | 'Mean Combine' >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
                               | 'format pdkit data' >> beam.Map(format_result)
                               )

        abs_energy_windowed | beam.ParDo(lambda (x): print(x))

# [END main]


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()