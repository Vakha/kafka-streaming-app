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
        T.StructField("timestamp", T.DateType(), False),
    ]
)

json_messages = df.select(
    F.from_json(F.decode(df.value, "UTF-8"), dataSchema).alias("json")
)

query = json_messages.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
