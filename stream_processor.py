from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType
from pyspark.sql.window import Window
import os

# --- 1. CONFIGURATION ---
KAFKA_BROKERS = 'localhost:9092'
KAFKA_TOPIC = 'clean_clickstream'
# JDBC Configuration for storing results (Example using PostgreSQL/MySQL)
# For simplicity in testing, we can use a console sink first.
# JDBC_URL = "jdbc:postgresql://<host>:<port>/<database>"
# JDBC_PROPERTIES = {
#     "user": "user",
#     "password": "password",
#     "driver": "org.postgresql.Driver"
# }
CHECKPOINT_LOCATION = "file:///tmp/spark/clickstream_checkpoint"


# --- 2. INITIALIZE SPARK SESSION ---
# Spark 4.0.1 is used
spark = SparkSession.builder \
    .appName("ClickstreamAnalytics") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define the schema matching the cleaned NiFi output
# Note: 'timestamp' is the original long, 'readable_timestamp' is the string created by NiFi
schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("visitorid", StringType(), True),
    StructField("event", StringType(), True),
    StructField("itemid", StringType(), True),
    StructField("readable_timestamp", StringType(), True),
])


# --- 3. READ FROM KAFKA STREAM ---
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize the JSON value using the defined schema
stream_df = kafka_df.select(
    col("key").cast("string").alias("session_key"),  # visitorid is our key
    from_json(col("value").cast("string"), schema).alias("value")
).select("session_key", "value.*")

# Cast the readable_timestamp string to a proper Timestamp type for calculations
stream_df = stream_df.withColumn(
    "event_time", 
    to_timestamp(col("readable_timestamp"), "yyyy-MM-dd HH:mm:ss.SSS")
)


# --- 4. SESSIONIZATION AND PATH ANALYSIS ---

# Define a window to order events within a session (visitorid)
# This is crucial for calculating time differences and path order
window_spec = Window.partitionBy("visitorid").orderBy("event_time")

# Calculate the time difference between the current event and the next event
# This gives us the "Time on Page"
analyzed_df = stream_df.withColumn(
    "next_event_time", 
    lead("event_time", 1).over(window_spec)
).withColumn(
    "time_on_page_seconds", 
    coalesce(
        col("next_event_time").cast("long") - col("event_time").cast("long"), 
        lit(0) # Use 0 for the last event in a micro-batch
    )
)

# Aggregate and calculate the total session path and duration
# We use collect_list and concat_ws to build the path string
session_df = analyzed_df.groupBy("visitorid") \
    .agg(
        concat_ws(" -> ", collect_list(col("event"))).alias("click_path"),
        sum(col("time_on_page_seconds")).alias("session_duration_seconds"),
        min(col("event_time")).alias("session_start_time"),
        count(col("event")).alias("event_count")
    ) \
    .withColumn("avg_time_per_event", col("session_duration_seconds") / col("event_count"))


# --- 5. WRITE THE RESULTS TO THE SINK (Database) ---

# Define the write stream query. For the first test, we'll use a Console Sink.
# This will print the results to your terminal every time a session is completed.
query = session_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

# For a production database sink, you would use:
# query = session_df.writeStream \
#     .foreachBatch(write_to_jdbc) \
#     .outputMode("update") \
#     .option("checkpointLocation", CHECKPOINT_LOCATION) \
#     .start()

print("Spark Streaming Job Started. Waiting for data...")

query.awaitTermination()