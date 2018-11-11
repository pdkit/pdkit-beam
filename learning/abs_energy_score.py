from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import csv
import logging
import sys
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger
from apache_beam.io import ReadFromText
import numpy as np


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
            | beam.Map(lambda elem: (elem[self.field], elem['mag_sum_acc']))
            | beam.CombinePerKey(sum))


# [START window_and_trigger]
class CalculateTeamScores(beam.PTransform):
  """Calculates scores for each team within the configured window duration.

  Extract team/score pairs from the event stream, using hour-long windows by
  default.
  """
  def __init__(self, team_window_duration, allowed_lateness):
    super(CalculateTeamScores, self).__init__()
    self.team_window_duration = team_window_duration * 60
    self.allowed_lateness_seconds = allowed_lateness * 60

  def expand(self, pcoll):
    # NOTE: the behavior does not exactly match the Java example
    # TODO: allowed_lateness not implemented yet in FixedWindows
    # TODO: AfterProcessingTime not implemented yet, replace AfterCount
    return (
        pcoll
        # We will get early (speculative) results as well as cumulative
        # processing of late data.
        | 'LeaderboardTeamFixedWindows' >> beam.WindowInto(
            beam.window.FixedWindows(self.team_window_duration),
            trigger=trigger.AfterWatermark(trigger.AfterCount(10),
                                           trigger.AfterCount(20)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
        # Extract and sum teamname/score pairs from the event data.
        | 'ExtractAndSumScore' >> ExtractAndSumScore('user'))
# [END window_and_trigger]


# [START main]
def run(argv=None):
    """Main entry point; defines and runs the PDkit_score pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='tremor_data_with_user.csv',
                        help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        events = ( p
                   | 'Read' >> ReadFromText(known_args.input)
                   | 'AddEventTimestamps' >> beam.ParDo(TransformTimestampDoFn())
                   )

        a = ( events
              | 'ParseAccEventFn' >> beam.ParDo(ParseAccEventFn())
              | 'CalculateTeamScores' >> CalculateTeamScores(
                    args.team_window_duration, args.allowed_lateness)
              )

        a | beam.ParDo(lambda (x): print(x))




if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()