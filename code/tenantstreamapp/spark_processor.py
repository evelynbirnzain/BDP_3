import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window, explode
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import QueryStartedEvent, QueryProgressEvent, QueryIdleEvent, QueryTerminatedEvent
from pymongo import MongoClient

from schema import schema

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.INFO)
rootLogger.addHandler(consoleHandler)

zookeeper_quorum = os.getenv('ZOOKEEPER')
consumer_group_id = 'pollution-monitor'
topic_report = 'report'
kafka = os.getenv('KAFKA')
kafka_topic = os.getenv('KAFKA_TOPIC')

mongo_url = "mongodb://mongodb:27017"

class Listener(StreamingQueryListener):

    def __init__(self):
        super().__init__()
        self.db_client = MongoClient(mongo_url)
        self.db = self.db_client["metrics"]
        self.collection = self.db["tenantstreamapp"]

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        rootLogger.info("Query started: %s", event.id)

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        rootLogger.info("Query progress: %s", event.progress)
        log_entry = {
            "runId": str(event.progress.runId),
            "timestamp": event.progress.timestamp,
            "numInputRows": event.progress.numInputRows,
            "inputRowsPerSecond": event.progress.inputRowsPerSecond,
            "processedRowsPerSecond": event.progress.processedRowsPerSecond,
            "durationMs": event.progress.durationMs,
            "batchDuration": event.progress.batchDuration
        }
        r = self.collection.insert_one(log_entry)
        rootLogger.info("Inserted log entry: %s", r.inserted_id)

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        rootLogger.info("Query idle: %s", event.id)
        log_entry = {
            "runId": str(event.id),
            "timestamp": event.timestamp,
            "state": "idle"
        }
        r = self.collection.insert_one(log_entry)
        rootLogger.info("Inserted log entry: %s", r.inserted_id)

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        rootLogger.info("Query terminated: %s", event.id)
        if event.exception:
            rootLogger.error("Query exception: %s", event.exception)
        else:
            rootLogger.info("Query duration: %s", event.duration)


def main():
    rootLogger.info("Starting Spark session")

    spark = SparkSession.builder \
        .appName("Pollution_Monitor") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logging.info("Spark session started")

    input_stream = load_input_stream(spark)
    input_stream.printSchema()  # kafka schema

    input_stream = deserialize_input_stream(input_stream)
    input_stream.printSchema()  # deserialized values

    input_stream = map_and_filter(input_stream)
    input_stream.printSchema()  # mapped and filtered values

    report_data = aggregate_data(input_stream)
    report_data.printSchema()  # aggregated values

    alert_data = report_data.filter("average > 100")

    report_sink = build_write_stream(report_data, topic_report)
    alert_sink = build_write_stream(alert_data, 'alert')

    spark.streams.addListener(Listener())
    spark.streams.awaitAnyTermination()


def load_input_stream(spark):
    input_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka) \
        .option("auto.offset.reset", "latest") \
        .option("startingOffsets", "latest") \
        .option("subscribe", kafka_topic) \
        .option("kafka.group.id", consumer_group_id) \
        .load()

    return input_stream


def deserialize_input_stream(input_stream):
    input_stream_json = input_stream.selectExpr("CAST(value AS STRING)")
    input_stream = input_stream_json \
        .withColumn("value", from_json(input_stream_json["value"], schema)) \
        .select("value.*")

    return input_stream


def map_and_filter(input_stream):
    input_stream = input_stream \
        .withColumn("sensordatavalues", explode("sensordatavalues")) \
        .select("id", "timestamp", "sensor", "sensordatavalues.*") \
        .filter("value_type == 'P1' or value_type == 'P2'")

    input_stream = input_stream \
        .withColumn("sensor_id", input_stream["sensor.id"]) \
        .withColumn("value", input_stream["value"].cast("float"))

    return input_stream


def aggregate_data(input_stream):
    report_data = input_stream \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy("sensor_id", window("timestamp", "15 minutes", "5 minutes"), "value_type") \
        .avg("value") \
        .withColumnRenamed("avg(value)", "average") \
        .select("sensor_id", "value_type", "average", "window") \
        .withColumnRenamed("value_type", "pollution_type") \
        .withColumnRenamed("average", "pollution_level")

    return report_data


def build_write_stream(data, topic):
    stream = data \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka) \
        .option("topic", topic) \
        .option("checkpointLocation", f"/tmp/checkpoint_{topic}_3") \
        .outputMode("complete") \
        .start()

    return stream

if __name__ == '__main__':
    main()
