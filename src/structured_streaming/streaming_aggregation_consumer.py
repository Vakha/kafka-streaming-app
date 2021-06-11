from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession.builder.appName("StructuredNetworkApp").config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
    )
    # to correctly parse time taking timezone in account.
    # Otherwise time is converted to spark timezone and original timezone information is gone.
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    # .option("kafka.group.id", "my-group")
    .option("subscribe", "output-topic-6")
    .option("startingOffsets", "earliest")
    .load()
)

dataSchema = T.StructType(
    [
        T.StructField("deviceId", T.IntegerType(), False),
        T.StructField("stage", T.StringType(), False),
        T.StructField("seqNum", T.IntegerType(), False),
        T.StructField("timestamp", T.TimestampType(), False),
    ]
)

messages = (
    df.select(F.decode(df.value, "UTF-8").alias("jsonStr"))
    .select(F.from_json(F.col("jsonStr"), dataSchema).alias("json"))
    .selectExpr("json.*")
)


def count_by_condition(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


messages_b = (
    messages
    .withWatermark("timestamp", "30 minutes")
    .groupBy(F.window("timestamp", "20 minutes", "5 minutes"), F.col("deviceId"), F.col("seqNum"))
    .agg(
        F.min("timestamp").alias("startTimestamp"),
        F.max("timestamp").alias("stopTimestamp"),
        count_by_condition(F.col("stage") == "START_PICKING").alias("startCount"),
        count_by_condition(F.col("stage") == "PICKED_APPLE").alias("pickingCount"),
        count_by_condition(F.col("stage") == "STOP_PICKING").alias("stopCount"),
    )
)


query = messages_b.writeStream.outputMode("update").format("console").option("truncate", "false").start()

query.awaitTermination()
