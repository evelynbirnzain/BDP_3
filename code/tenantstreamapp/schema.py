from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, TimestampType

sensor_schema = StructType([
    StructField("id", StringType(), False),
    StructField("pin", StringType(), True),
    StructField("sensor_type", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("manufacturer", StringType(), True)
    ]), True),
])

sensor_data_values_schema = ArrayType(StructType([
    StructField("id", StringType(), False),
    StructField("value", StringType(), False),
    StructField("value_type", StringType(), False)
]))

location_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("altitude", StringType(), True),
    StructField("country", StringType(), True),
    StructField("exact_location", BooleanType(), True),
    StructField("indoor", BooleanType(), True)
])

schema = StructType([
    StructField("id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("location", location_schema, False),
    StructField("sensor", sensor_schema, False),
    StructField("sensordatavalues", sensor_data_values_schema, False),
])