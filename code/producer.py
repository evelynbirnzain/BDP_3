import os

from kafka import KafkaProducer
import logging
import json
import sys
import time
import dotenv

dotenv.load_dotenv()

KAFKA_HOST = os.getenv('KAFKA_HOST')
KAFKA_PORT = os.getenv('KAFKA_PORT')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

print(f"KAFKA_HOST: {KAFKA_HOST}")
print(f"KAFKA_PORT: {KAFKA_PORT}")
print(f"KAFKA_TOPIC: {KAFKA_TOPIC}")


class KafkaPublisher:

    def __init__(self, config, logger):
        super().__init__()
        self.logger = logger
        logger.info(f"Connecting to Kafka at {config}")

        try:
            self._producer = KafkaProducer(bootstrap_servers=config)
            self.logger.info("Connected to Kafka")
        except Exception as e:
            self.logger.error(f"Error occurred while connecting: {e}")

    def produce(self, topic, value):
        self.logger.debug(f"Publishing to topic: {topic}")
        ack = self._producer.send(topic, str.encode(json.dumps(value)), key=str.encode(str(value["sensor"]["id"])))
        ack.add_callback(lambda x: self.logger.info(f"Message sent: {x}"))
        ack.add_errback(lambda x: self.logger.error(f"Error sending message: {x}"))

    def stop(self):
        self.logger.info("Stopping producer")
        self._producer.flush()
        self._producer.close()
        self.logger.info("Producer stopped")


if __name__ == '__main__':
    logfile = "logs/producer.log"
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])

    logger = logging.getLogger()
    publisher = KafkaPublisher(f'{KAFKA_HOST}:{KAFKA_PORT}', logger)

    logger.info("Starting to send messages")

    with open("data/sensor-data.json", 'r') as f:
        measurements = json.load(f)

    try:
        while True:
            for measurement in measurements:
                publisher.produce(KAFKA_TOPIC, measurement)
                if len(sys.argv) > 1:
                    time.sleep(float(sys.argv[1]))
    except KeyboardInterrupt:
        publisher.stop()
