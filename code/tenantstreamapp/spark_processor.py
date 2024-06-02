import logging
import os

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window, explode
from schema import schema

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger("mini-batcher-application")
rootLogger.setLevel(logging.DEBUG)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.DEBUG)
rootLogger.addHandler(consoleHandler)

zookeeper_quorum = os.getenv('ZOOKEEPER')
consumer_group_id = 'spark-streaming'
topic_report = 'report'
kafka = os.getenv('KAFKA')
kafka_topic = os.getenv('KAFKA_TOPIC')


def GetKafkaProducer():
    return KafkaProducer(bootstrap_servers=[kafka])


def send_alert(data):
    producer = GetKafkaProducer()
    producer.send(topic_report, str.encode(data))
    producer.flush()

spark = SparkSession.builder \
    .appName("P1_Monitor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

input_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka) \
    .option("auto.offset.reset", "latest") \
    .option("startingOffsets", "latest") \
    .option("subscribe", kafka_topic) \
    .load()

input_stream.printSchema()  # default kafka schema

input_stream_json = input_stream.selectExpr("CAST(value AS STRING)")
input_stream = input_stream_json \
    .withColumn("value", from_json(input_stream_json["value"], schema)) \
    .select("value.*")

input_stream.printSchema()  # custom schema

input_stream = input_stream \
    .withColumn("sensordatavalues", explode("sensordatavalues")) \
    .select("id", "timestamp", "location", "sensor", "sensordatavalues.*") \
    .filter("value_type == 'P1'")

input_stream = input_stream.withColumn("value", input_stream["value"].cast("float"))
input_stream = input_stream.withColumn("sensor_id", input_stream["sensor.id"])

windowed_data = input_stream \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "5 minutes"), "sensor_id") \
    .avg("value") \
    .withColumnRenamed("avg(value)", "average") \
    .select("sensor_id", "window.start", "window.end", "average") \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

windowed_data.awaitTermination()
