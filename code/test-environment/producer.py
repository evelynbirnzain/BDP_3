import os

from kafka import KafkaProducer
import logging
import json
import sys
import time
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta
import random
import argparse
from tqdm import tqdm

load_dotenv()

KAFKA_HOST = os.getenv('KAFKA_HOST')
KAFKA_PORT = os.getenv('KAFKA_PORT')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

logfile = "logs/producer.log"
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])
logger = logging.getLogger()


class KafkaPublisher:

    def __init__(self, config, logger):
        super().__init__()
        self.logger = logger
        logger.info(f"Connecting to Kafka at {config}")

        self._producer = KafkaProducer(bootstrap_servers=config)
        self.logger.info("Connected to Kafka")

    def produce(self, topic, value):
        self.logger.debug(f"Publishing to topic: {topic}")
        ack = self._producer.send(topic, str.encode(json.dumps(value)), key=str.encode(str(value["sensor"]["id"])))
        ack.add_callback(lambda x: self.logger.debug(f"Message sent: {x}"))
        ack.add_errback(lambda x: self.logger.error(f"Error sending message: {x}"))

    def stop(self):
        self.logger.info("Stopping producer")
        self._producer.flush()
        self._producer.close()
        self.logger.info("Producer stopped")


def get_measurement(measurements, wrong_measurements, num_sensors, p_extra_fields, p_missing_values,
                    p_wrong_measurement):
    if random.random() < p_wrong_measurement:
        measurement = random.choice(wrong_measurements)
    else:
        measurement = random.choice(measurements)

    if random.random() < p_extra_fields:
        measurement["extra_field"] = "extra_value"

    if random.random() < p_missing_values:
        measurement["sensordatavalues"][0]["value"] = None

    event_time = datetime.now() - timedelta(seconds=random.randint(0, 100))
    measurement["timestamp"] = event_time.strftime("%Y-%m-%d %H:%M:%S")
    measurement["sensor"]["id"] = random.randint(1, int(num_sensors))
    return measurement


if __name__ == '__main__':
    try:
        publisher = KafkaPublisher(f'{KAFKA_HOST}:{KAFKA_PORT}', logger)
    except Exception as e:
        logger.error(f"Error connecting to Kafka: {e}")
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument("--n_sensors", help="Number of sensors to simulate")
    parser.add_argument("--delay", help="Delay between messsages")
    parser.add_argument("--p_extra", help="Probability of measurements with extra fields")
    parser.add_argument("--p_missing", help="Probability of measurements with missing values")
    parser.add_argument("--p_wrong", help="Probability of wrong measurements (right schema, wrong measurement types)")
    args = parser.parse_args()

    logger.info("Starting to send messages")

    with open("data/pollution-sensor-data.json", 'r') as f:
        measurements = json.load(f)

    with open("data/other-sensor-data.json", 'r') as f:
        wrong_measurements = json.load(f)

    num_sensors = int(args.n_sensors) if args.n_sensors else 10
    p_extra_fields = float(args.p_extra) if args.p_extra else 0
    p_missing_values = float(args.p_missing) if args.p_missing else 0
    p_wrong_measurement = float(args.p_wrong) if args.p_wrong else 0
    delay = float(args.delay) if args.delay else 0

    logger.info(
        f"Sending messages with {num_sensors} sensors, p_extra={p_extra_fields}, p_missing={p_missing_values}, p_wrong={p_wrong_measurement} at a rate of ~{1 / delay} messages per second")

    try:
        while True:
            start = time.time()
            for i in tqdm(range(100000)):
                measurement = get_measurement(measurements, wrong_measurements, num_sensors, p_extra_fields,
                                              p_missing_values, p_wrong_measurement)
                publisher.produce(KAFKA_TOPIC, measurement)
                elapsed = time.time() - start
                target = (i+1) * delay
                if elapsed < target:
                    time.sleep(target - elapsed)
                if i % 1000 == 0:
                    logging.info("Actual rate: %s", i / (time.time() - start))
    except KeyboardInterrupt:
        publisher.stop()
