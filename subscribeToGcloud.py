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


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic',
                        help='Input PubSub topic of the form "projects/<PROJECT>/subscription/<TOPIC>".')
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Read from PubSub into a PCollection.
        if known_args.input_topic:
            messages = (p
                        | 'Read from Stream' >> beam.io.gcp.pubsub.ReadFromPubSub(subscription=known_args.input_topic)
                        .with_output_types(bytes)
                        )

            messages | beam.ParDo(lambda (x): print(x))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()