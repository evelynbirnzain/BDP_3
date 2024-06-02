from time import sleep

from kafka import KafkaProducer
from kafka.errors import KafkaError
from threading import Thread
import logging
import json


def on_send_success(record_metadata):
    LOGGER.info("Published to Kafka")


def on_send_error(excp):
    LOGGER.error('I am an errback', exc_info=excp)

LOGGER = logging.getLogger("KafkaPublisher")
LOGGER.setLevel(logging.INFO)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
LOGGER.addHandler(consoleHandler)



class KafkaPublisher:

    logger = LOGGER
    def __init__(self, config):
        super().__init__()
        try:
            self._producer = KafkaProducer(bootstrap_servers=config)
            self.logger.info("Connected to Kafka")
        except Exception as e:
            self.logger.error(f"Error occured while connecting: {e}")

    # Produce Async and handle exception
    def produce(self, topic, value):
        self.logger.info(f"Publishing to topic: {topic}")
        try:
            ack = self._producer.send(topic, str.encode(json.dumps(value)))
            res = ack.get(2)
            self.logger.info(f"Published to Kafka: {res}")
            ack.add_callback(on_send_success)
            ack.add_errback(on_send_error)

        except Exception as e:
            self.logger.error(f"Encountered error while trying to publish: {e}")


if __name__ == '__main__':
    publisher = KafkaPublisher("192.168.0.21:9092")

    LOGGER.info("Starting to send messages")
    while True:
        publisher.produce("sensors.temperature.6", {"name": "test_report"})
        sleep(1)

    print("All messages sent")
