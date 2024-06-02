from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, TimestampType

sensor_schema = StructType([
    StructField("id", StringType(), False),
    StructField("pin", StringType(), False),
    StructField("sensor_type", StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("manufacturer", StringType(), False)
    ]), False),
])

sensor_data_values_schema = ArrayType(StructType([
    StructField("id", StringType(), False),
    StructField("value", StringType(), False),
    StructField("value_type", StringType(), False)
]))

location_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("latitude", StringType(), False),
    StructField("longitude", StringType(), False),
    StructField("altitude", StringType(), False),
    StructField("country", StringType(), False),
    StructField("exact_location", BooleanType(), False),
    StructField("indoor", BooleanType(), False)
])

schema = StructType([
    StructField("id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("location", location_schema, False),
    StructField("sensor", sensor_schema, False),
    StructField("sensordatavalues", sensor_data_values_schema, False),
])