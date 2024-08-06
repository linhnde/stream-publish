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


def publish(publisher_name, topic, events):
    num_obs = len(events)
    if num_obs > 0:
        logging.info(f"Publishing {num_obs} events from {get_timestamp(events[0])}")
        for event_data in events:
            publisher_name.publish(topic, event_data)


def get_timestamp(line):
    # convert from bytes to str
    line = line.decode('utf-8')

    # look at first field of row
    timestamp = line.split(',')[0]
    return datetime.datetime.strptime(timestamp, TIME_FORMAT)


def simulate(topic, items, firstObservedTime, programStart, speedFactor):
    # sleep computation
    def compute_sleep_secs(observed_time):
        # Current time minus starting time, converting to seconds
        time_elapsed_secs = (datetime.datetime.now(datetime.UTC) - programStart).seconds
        print(f"time_elapsed_secs = {time_elapsed_secs}")
        # Current time of line minus time of first line
        sim_time_elapsed = observed_time - firstObservedTime
        print(f"sim_time_elapsed = {sim_time_elapsed}")
        # Convert to seconds and increase speed of streaming
        # i.e. speedFactor=60 meaning instead of streaming every 5 minutes, it would be 5 seconds
        sim_time_elapsed_secs = (sim_time_elapsed.days * 60 * 60 * 24 + sim_time_elapsed.seconds) / speedFactor
        print(f"sim_time_elapsed_secs = {sim_time_elapsed_secs}")
        to_sleep_secs = sim_time_elapsed_secs - time_elapsed_secs
        print(f"to_sleep_secs = {to_sleep_secs}")
        return to_sleep_secs

    to_publish = list()

    for line in items:
        event_data = line  # entire line of input CSV is the message
        obs_time = get_timestamp(line)  # from first column

        # Not publish first observed time, but append it to waiting list for publishing in the next time frame
        if compute_sleep_secs(obs_time) > 1:
            publish(publisher, topic, to_publish)  # Notify the accumulated to_publish
            to_publish = list()  # Empty out list

            to_sleep_time = compute_sleep_secs(obs_time)  # Recompute sleep, since notification takes a while
            if to_sleep_time > 0:
                logging.info(f"Sleeping {to_sleep_time} seconds")
                time.sleep(to_sleep_time)
        to_publish.append(event_data)

    # left-over records; notify again
    publish(publisher, topic, to_publish)


def peek_timestamp(items):
    # Peek ahead to next line, get timestamp and go back, only use to get timestamp of first event
    pos = items.tell()
    line = items.readline()
    items.seek(pos)
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
    print(f"programStartTime = {programStartTime}")
    with gzip.open(INPUT, 'rb') as data_to_publish:
        header = data_to_publish.readline()     # Skip header
        firstObsTime = peek_timestamp(data_to_publish)      # Get timestamp of first event
        logging.info(f'Sending sensor data from {firstObsTime}')
        simulate(topic_path, data_to_publish, firstObsTime, programStartTime, args.speedFactor)
