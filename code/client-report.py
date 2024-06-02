from kafka import KafkaConsumer
import sys
import logging
import dotenv
import os

dotenv.load_dotenv()

KAFKA_HOST = os.getenv('KAFKA_HOST')

bootstrap_servers = KAFKA_HOST.split(",")
topicName = "report"

logfile = f"logs/client-report.log"
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])

logger = logging.getLogger()

logger.info("Starting to listen for messages on topic : " + topicName + ". ")

consumer = KafkaConsumer(topicName, bootstrap_servers=bootstrap_servers, auto_offset_reset='latest')

logger.info("Successfully connected to kafka consumer process!")

try:
    for message in consumer:
        logger.info("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
except KeyboardInterrupt:
    logger.info("Stopping consumer")
    consumer.close()
    logger.info("Consumer stopped")
