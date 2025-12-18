from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, to_timestamp, from_json
from pyspark.sql.types import StructType, StringType

schema = StructType().add("username", StringType()).add("timestamp", StringType())
spark = SparkSession.builder.appName("TelegramAnalytics").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","telegram_messages").load().selectExpr("CAST(value AS STRING) as json_str")
parsedf = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*").withColumn("timestamp", to_timestamp(col("timestamp")))

query1 = parsedf.groupBy( window(col("timestamp"),"1 minute","30 seconds"),col("username")).count().orderBy(col("count").desc())
query2 = parsedf.groupBy(window(col("timestamp"),"10 minutes","30 seconds"),col("username")).count().orderBy(col("count").desc())

q1 = query1.writeStream.outputMode("complete").format("console").option("truncate", False).start()
q2 = query2.writeStream.outputMode("complete").format("console").option("truncate", False).start()

q1.awaitTermination()
q2.awaitTermination()