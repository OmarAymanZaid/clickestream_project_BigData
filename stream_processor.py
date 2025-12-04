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

# JDBC_URL = "jdbc:mysql://localhost:3306/clickstreamdb"
# DB_PROPERTIES = {
#     "user": "root",
#     "password": "",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

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
# ANALYSIS 1: Full Sessionization
# -------------------------------------------------------------
def compute_sessionization(batch_df):
    from pyspark.sql.window import Window

    # Window per user ordered by event time
    w = Window.partitionBy("visitorid").orderBy("event_time")
    w_unbounded = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # STEP 1 — previous event + time difference
    df1 = (
        batch_df
        .withColumn("prev_event_time", lag("event_time").over(w))
        .withColumn(
            "diff_seconds",
            unix_timestamp("event_time") - unix_timestamp("prev_event_time")
        )
    )

    # STEP 2 — detect new session start
    df2 = (
        df1.withColumn(
            "is_new_session",
            when(col("prev_event_time").isNull(), 1)
            .when(col("diff_seconds") > 1800, 1)  # 30 minutes
            .otherwise(0)
        )
    )

    # STEP 3 — cumulative session number per user
    df3 = df2.withColumn(
        "session_number",
        sum("is_new_session").over(w_unbounded)
    )

    # STEP 4 — create global session_id
    df4 = df3.withColumn(
        "session_id",
        concat_ws("_", col("visitorid"), col("session_number"))
    )

    # STEP 5 — aggregate to session-level metrics
    session_df = (
        df4.groupBy("session_id", "visitorid")
           .agg(
               min("event_time").alias("session_start"),
               max("event_time").alias("session_end"),
               count("*").alias("events_in_session")
           )
           .withColumn(
               "session_length",
               unix_timestamp("session_end") - unix_timestamp("session_start")
           )
    )

    # Write to PostgreSQL
    session_df.write.jdbc(
        url=JDBC_URL,
        table="sessions",
        mode="append",
        properties=DB_PROPERTIES
    )

    return df4


# -------------------------------------------------------------
# ANALYSIS 2: User Path Construction (Per Session)
# -------------------------------------------------------------

def compute_user_paths(sessionized_df):
    """
    Takes a sessionized batch dataframe (must contain session_id)
    and builds ordered user event paths per session.
    """
    from pyspark.sql.functions import col, struct, collect_list, array_sort, expr

    # 1. Create struct(event_time, event)
    df_with_struct = sessionized_df.withColumn(
        "event_struct",
        struct(col("event_time"), col("event"))
    )

    # 2. Group by visitor + session
    grouped = df_with_struct.groupBy(
        "visitorid", "session_id"
    ).agg(
        collect_list("event_struct").alias("events")
    )

    # 3. Sort events by event_time
    ordered = grouped.withColumn(
        "ordered_events",
        array_sort(col("events"))
    )

    # 4. Extract only the event names from sorted structs
    result = ordered.withColumn(
        "user_path",
        expr("transform(ordered_events, x -> x.event)")
    ).select(
        "visitorid", "session_id", "user_path"
    )

    # Write to PostgreSQL
    result.write.jdbc(
        url=JDBC_URL,
        table="user_paths",
        mode="append",
        properties=DB_PROPERTIES
    )

# -------------------------------------------------------------
# ANALYSIS 3: Funnel Analysis
# -------------------------------------------------------------

def compute_funnel_analysis(sessionized_df, batch_id, funnel_steps=None):
    """
    Computes a simple funnel: counts number of sessions/users that reached each step.
    sessionized_df must contain: visitorid, session_id, event
    batch_id: integer, identifies the current streaming batch
    funnel_steps: ordered list of events in the funnel
    """
    from pyspark.sql import DataFrame

    if funnel_steps is None:
        funnel_steps = ["view", "addtocart", "transaction"]

    # 1. Aggregate events per session (distinct events per session)
    session_events = (
        sessionized_df.groupBy("session_id", "visitorid")
        .agg(collect_list("event").alias("events"))
    )

    # 2. Create columns for each funnel step (1 if session contains the event)
    for step in funnel_steps:
        session_events = session_events.withColumn(
            step,
            when(array_contains(col("events"), step), 1).otherwise(0)
        )

    # 3. Aggregate counts per funnel step
    funnel_counts = session_events.agg(
        *[sum(step).alias(step) for step in funnel_steps]
    )

    # 4. Add batch_id and analysis_time
    funnel_counts = funnel_counts.withColumn("batch_id", lit(batch_id)) \
                                 .withColumn("analysis_time", current_timestamp()) \
                                 .select("batch_id", "analysis_time", *funnel_steps)

    # 5. Write to PostgreSQL
    funnel_counts.write.jdbc(
        url=JDBC_URL,
        table="funnel_analysis",
        mode="append",
        properties=DB_PROPERTIES
    )

# -------------------------------------------------------------
# ANALYSIS 4: EVENTS PER MINUTE
# -------------------------------------------------------------
def compute_events_per_minute(batch_df):
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
# ANALYSIS 5: Active Users in a minute
# -------------------------------------------------------------

def compute_active_users(df):
    result = (
        df.withColumn("minute", date_trunc("minute", col("event_time")))
          .groupBy("minute")
          .agg(count_distinct("visitorid").alias("active_users"))
    )

    result.write.jdbc(
        url=JDBC_URL,
        table="active_users",
        mode="append",
        properties=DB_PROPERTIES
    )

# -------------------------------------------------------------
# ANALYSIS 6: Event Type Distribution
# -------------------------------------------------------------
def compute_event_type_distribution(batch_df):
    result = (
        batch_df.withColumn("minute", date_trunc("minute", col("event_time")))
                .groupBy("minute", "event")
                .count()
                .withColumnRenamed("count", "event_count")
    )

    result.write.jdbc(
        url=JDBC_URL,
        table="event_type_distribution",
        mode="append", 
        properties=DB_PROPERTIES
    )

# -------------------------------------------------------------
# ANALYSIS 7: Bounce Rate (Users with Only 1 Event)
# -------------------------------------------------------------
def compute_bounce_rate(batch_df):
    minute_df = batch_df.withColumn("minute", date_trunc("minute", col("event_time")))

    per_user = (
        minute_df.groupBy("minute", "visitorid")
                 .count()
                 .withColumnRenamed("count", "events")
    )

    result = (
        per_user.groupBy("minute")
                .agg(
                    sum(when(col("events") == 1, 1).otherwise(0)).alias("bounces"),
                    count("*").alias("total_users")
                )
                .withColumn("bounce_rate", col("bounces") / col("total_users"))
    )

    result.write.jdbc(
        url=JDBC_URL,
        table="bounce_rate",
        mode="append", 
        properties=DB_PROPERTIES
    )

# -------------------------------------------------------------
# ANALYSIS 8: Top Items Per Minute
# -------------------------------------------------------------
def compute_top_items(batch_df):
    result = (
        batch_df.withColumn("minute", date_trunc("minute", col("event_time")))
                .groupBy("minute", "itemid")
                .count()
                .withColumnRenamed("count", "interactions")
    )

    result.write.jdbc(
        url=JDBC_URL,
        table="top_items",
        mode="append", 
        properties=DB_PROPERTIES
    )



# -------------------------------------------------------------
# ANALYSIS 9: Item Interaction Counts
# -------------------------------------------------------------
def compute_item_interactions(batch_df):
    """
    Counts all events per item in the batch (views, clicks, etc.)
    """
    item_counts = (
        batch_df.groupBy("itemid")
                .agg(count("*").alias("interaction_count"))
    )

    # Write to PostgreSQL
    item_counts.write.jdbc(
        url=JDBC_URL,
        table="item_interactions",
        mode="append",
        properties=DB_PROPERTIES
    )

    return item_counts


# -------------------------------------------------------------
# ANALYSIS 10: Most Viewed Items
# -------------------------------------------------------------
def compute_most_viewed_items(batch_df):
    """
    Counts number of views per item in the batch.
    """

    viewed_df = (
        batch_df.filter(col("event") == "view")
                .groupBy("itemid")
                .agg(count("*").alias("view_count"))
    )

    # Write to PostgreSQL
    viewed_df.write.jdbc(
        url=JDBC_URL,
        table="most_viewed_items",
        mode="append",
        properties=DB_PROPERTIES
    )

    return viewed_df


# -------------------------------------------------------------
# START STREAM
# -------------------------------------------------------------
def run_all_analyses(batch_df, batch_id):
    print(f"Running analyses for batch {batch_id}")

    # compute_events_per_minute(batch_df)
    # compute_active_users(batch_df)
    # compute_event_type_distribution(batch_df)
    # compute_top_items(batch_df)
    # compute_bounce_rate(batch_df)
    # compute_session_length(batch_df)

    sessionized_df = compute_sessionization(batch_df)
    compute_user_paths(sessionized_df)
    compute_funnel_analysis(sessionized_df, batch_id)
    compute_item_interactions(batch_df)
    compute_most_viewed_items(batch_df)

query = (
    stream_df.writeStream
        .foreachBatch(run_all_analyses)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
)

query.awaitTermination()

