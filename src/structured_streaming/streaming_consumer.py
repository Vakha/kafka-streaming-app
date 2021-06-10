from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession.builder.appName("StructuredNetworkApp").config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
    )
    # .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")
    .getOrCreate()
)
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    # .option("kafka.group.id", "my-group")
    .option("subscribe", "output-topic-2")
    .option("startingOffsets", "earliest")
    .load()
)

dataSchema = T.StructType(
    [
        T.StructField("deviceId", T.IntegerType(), False),
        T.StructField("stage", T.StringType(), False),
        T.StructField("timestamp", T.TimestampType(), False),
    ]
)

messages = (
    df.select(F.decode(df.value, "UTF-8").alias("jsonStr"))
    .select(F.from_json(F.col("jsonStr"), dataSchema).alias("json"))
    .select(
        F.col("json.deviceId"), F.col("json.stage"), F.col("json.timestamp")
    )
)

query = messages.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
