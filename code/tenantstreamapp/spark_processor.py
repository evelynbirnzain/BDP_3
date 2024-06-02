import logging
import os

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

input_stream.printSchema()  # deserialized values

input_stream = input_stream \
    .withColumn("sensordatavalues", explode("sensordatavalues")) \
    .select("id", "timestamp", "location", "sensor", "sensordatavalues.*") \
    .filter("value_type == 'P1' or value_type == 'P2'")

input_stream = input_stream.withColumn("value", input_stream["value"].cast("float"))
input_stream = input_stream.withColumn("sensor_id", input_stream["sensor.id"])

windowed_data = input_stream \
    .withWatermark("timestamp", "1 minute") \
    .groupBy("sensor_id", window("timestamp", "1 minute"), "value_type") \
    .avg("value") \
    .withColumnRenamed("avg(value)", "average")

# if value > 20 then send to alert topic; send all data to "report" topic

alert_data = windowed_data \
    .filter("average > 100") \
    .selectExpr("to_json(struct(*)) AS value")

report_data = windowed_data \
    .selectExpr("to_json(struct(*)) AS value")

alert_sink = alert_data \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka) \
    .option("topic", "alert") \
    .option("checkpointLocation", "/tmp/checkpoint-alert") \
    .outputMode("complete") \
    .start()

report_sink = report_data \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka) \
    .option("topic", topic_report) \
    .option("checkpointLocation", "/tmp/checkpoint-report") \
    .outputMode("complete") \
    .start()

alert_sink.awaitTermination()
report_sink.awaitTermination()

#mongo_sink = windowed_data \
#    .writeStream \
#    .format("mongodb") \
#    .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017/") \
#    .option("checkpointLocation", "/tmp/checkpoint") \
#    .outputMode("append") \
#    .option("replaceDocument", "false") \
#    .option("connection.uri", "mongodb://mongodb:27017/") \
#    .option("database", "tenantstream") \
#    .option("collection", "p1_monitor") \
#    .start()

#mongo_sink.awaitTermination()