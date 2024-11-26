from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import explode, length, split, col

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("StructuredStreaming").master("spark://spark:7077").getOrCreate()

# Config spark 
spark.conf.set("spark.sql.shuffle.partitions", "50")
spark.conf.set("spark.sql.adaptive.enabled", "false")


# Đọc stream từ socket
lines = spark \
    .readStream \
    .format("socket") \
    .option("host","netcat") \
    .option("port",9999) \
    .load()

# Tách các từ
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

# Query 1: All word counts
query1 = wordCounts \
    .writeStream \
    .queryName("wordCounts") \
    .outputMode("complete") \
    .format("console") \
    .start()

# Query 2: Words with even counts
evenCounts = wordCounts.filter(col("count") % 2 == 0)
query2 = evenCounts \
    .writeStream \
    .queryName("evenCounts") \
    .outputMode("complete") \
    .format("console") \
    .start()


# Query 3: Words with length > 1 and odd counts
oddLengthWords = wordCounts.filter((length(col("word")) > 1) & (col("count") % 2 != 0))
query3 = oddLengthWords \
    .writeStream \
    .queryName("oddLengthWords") \
    .outputMode("complete") \
    .format("console") \
    .start()

# Chờ các query hoàn thành
query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()

