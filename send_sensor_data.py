#!/usr/bin/env python3

# Copyright 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import argparse
import datetime
import gzip
import logging
import time

from google.cloud import pubsub
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pubsub-credentials.json"

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
TOPIC = 'test_publish'
INPUT = 'sensor_obs2008.csv.gz'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pubsub-credentials.json"


def publish(publisher, topic, events):
    numobs = len(events)
    if numobs > 0:
        logging.info(f"Publishing {numobs} events from {get_timestamp(events[0])}")
        for event_data in events:
            publisher.publish(topic, event_data)


def get_timestamp(line):
    # convert from bytes to str
    line = line.decode('utf-8')

    # look at first field of row
    timestamp = line.split(',')[0]
    return datetime.datetime.strptime(timestamp, TIME_FORMAT)


def simulate(topic, ifp, firstObsTime, programStart, speedFactor):
    # sleep computation
    def compute_sleep_secs(obs_time):
        time_elapsed = (datetime.datetime.now(datetime.UTC) - programStart).seconds
        sim_time_elapsed = ((obs_time - firstObsTime).days * 86400.0 + (obs_time - firstObsTime).seconds) / speedFactor
        to_sleep_secs = sim_time_elapsed - time_elapsed
        return to_sleep_secs

    to_publish = list()

    for line in ifp:
        event_data = line  # entire line of input CSV is the message
        obs_time = get_timestamp(line)  # from first column

        # how much time should we sleep?
        if compute_sleep_secs(obs_time) > 1:
            # notify the accumulated to_publish
            publish(publisher, topic, to_publish)  # notify accumulated messages
            to_publish = list()  # empty out list

            # recompute sleep, since notification takes a while
            to_sleep_secs = compute_sleep_secs(obs_time)
            if to_sleep_secs > 0:
                logging.info(f"Sleeping {to_sleep_secs} seconds")
                time.sleep(to_sleep_secs)
        to_publish.append(event_data)

    # left-over records; notify again
    publish(publisher, topic, to_publish)


def peek_timestamp(ifp):
    # peek ahead to next line, get timestamp and go back
    pos = ifp.tell()
    line = ifp.readline()
    ifp.seek(pos)
    return get_timestamp(line)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Send sensor data to Cloud Pub/Sub in small groups, simulating real-time behavior')
    parser.add_argument('--speedFactor', help='Example: 60 implies 1 hour of data sent to Cloud Pub/Sub in 1 minute',
                        required=True, type=float)
    parser.add_argument('--project', help='Example: --project $DEVSHELL_PROJECT_ID', required=True)
    args = parser.parse_args()

    # create Pub/Sub notification topic
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(args.project, TOPIC)
    try:
        publisher.get_topic(request={"name": topic_path})
        logging.info(f"Reusing pub/sub topic {TOPIC}")
    except:
        publisher.create_topic(request={"name": topic_path})
        logging.info(f"Creating pub/sub topic '{TOPIC}'")

    # notify about each line in the input file
    programStartTime = datetime.datetime.now(datetime.UTC)
    with gzip.open(INPUT, 'rb') as ifp:
        header = ifp.readline()  # skip header
        firstObsTime = peek_timestamp(ifp)
        logging.info(f'Sending sensor data from {firstObsTime}')
        simulate(topic_path, ifp, firstObsTime, programStartTime, args.speedFactor)
