from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# -------------------------
# CONFIG
# -------------------------
KAFKA_BROKERS = "localhost:9092"
KAFKA_TOPIC = "clean_clickstream"
CHECKPOINT_LOCATION = "file:///tmp/spark/clickstream_checkpoint"
JDBC_URL = "jdbc:postgresql://localhost:5432/clickstreamdb"
DB_PROPERTIES = {
    "user": "postgres",
    "password": "123456789",
    "driver": "org.postgresql.Driver"
}

# -------------------------
# SPARK SESSION
# -------------------------
spark = SparkSession.builder \
    .appName("ClickstreamAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -------------------------
# SCHEMA
# -------------------------
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("visitorid", StringType(), True),
    StructField("event", StringType(), True),
    StructField("itemid", StringType(), True),
    StructField("transactionid", StringType(), True),
    StructField("event_category", StringType(), True),
    StructField("unix_timestamp", StringType(), True)
])

# -------------------------
# READ KAFKA STREAM
# -------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

stream_df = kafka_df.select(
    col("key").cast("string").alias("session_key"),
    from_json(col("value").cast("string"), schema).alias("value")
).select("session_key", "value.*")

# Convert unix timestamp (string) → long → timestamp
stream_df = stream_df.withColumn(
    "event_time", (col("unix_timestamp").cast("long") / 1000).cast("timestamp")
)



print("Spark Streaming Job Started...")

# -------------------------------------------------------------
# ANALYSIS 1: EVENTS PER MINUTE
# -------------------------------------------------------------
def compute_events_per_minute(batch_df, batch_id):
    print(f"Processing batch {batch_id}")

    result = (
        batch_df.withColumn("minute", date_trunc("minute", col("event_time")))
                .groupBy("minute")
                .count()
                .withColumnRenamed("count", "events_count")
    )

    # Write to PostgreSQL
    result.write.jdbc(
        url=JDBC_URL,
        table="events_per_minute",
        mode="append",
        properties=DB_PROPERTIES
    )

# -------------------------------------------------------------
# START STREAM
# -------------------------------------------------------------
query = (
    stream_df.writeStream
        .outputMode("append")
        .foreachBatch(compute_events_per_minute)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
)

query.awaitTermination()

