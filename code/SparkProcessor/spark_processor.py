import logging
from pyspark.sql import SparkSession

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger("mini-batcher-application")
rootLogger.setLevel(logging.DEBUG)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.DEBUG)
rootLogger.addHandler(consoleHandler)

WINDOW_DURATION = 5
zookeeper_quorum = 'zookeeper:2181'
consumer_group_id = 'spark-streaming'
topic_report = 'report'
broker = "kafka:9092"

spark = SparkSession.builder\
    .appName("Pyspark_Temperature_Monitor")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

input_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", broker) \
    .option("auto.offset.reset", "latest") \
    .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .option("subscribe", "sensors.temperature.6") \
    .load()

input_stream.printSchema()

input_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()
