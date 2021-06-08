from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession
    .builder
    .appName("StructuredNetworkApp")
    .config(
        'spark.jars.packages',
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2'
    )
    .getOrCreate()
)
df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("kafka.group.id", "my-group")
    .option("subscribe", "output-topic")
    .option("startingOffsets", "earliest")
    .load()
)


query = (
    df
    .writeStream
    .outputMode("append")
    .format("console")
    .start()
)

query.awaitTermination()