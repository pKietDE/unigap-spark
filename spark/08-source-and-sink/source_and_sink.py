from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("source-and-sink").master("spark://spark:7077").getOrCreate()

flight_time_df = spark.read.parquet("/data/source-and-sink/flight-time.parquet")

flight_time_df.printSchema()

flight_time_df.show(1)

print(flight_time_df.rdd.getNumPartitions())

flight_time_df.groupBy(spark_partition_id()).count().show()

partitioned_df = flight_time_df.repartition(5)
print(f"Num Partitions after: {partitioned_df.rdd.getNumPartitions()}")
partitioned_df.groupBy(spark_partition_id()).count().show()

# Viết chương trình đọc dữ liệu từ thư mục avro tạo được trong ví dụ trên và lấy ra danh sách các hãng bay OP_CARRIER, ORIGIN và số chuyến bay bị hủy
# Dữ liệu sắp theo theo OP_CARRIER và ORIGIN.
partitioned_df = flight_time_df \
    .filter(col("CANCELLED") == 1) \
    .groupBy("OP_CARRIER", "ORIGIN") \
    .agg(count("*").alias("NUM_CANCELLED_FLIGHT")) \
    .orderBy("OP_CARRIER", "ORIGIN")

# Ghi kết quả
partitioned_df.write \
    .format("avro") \
    .mode("overwrite") \
    .option("path", "/data/sink/avro/") \
    .save()

# Viết chương trình đọc dữ liệu từ thư mục json tạo được trong ví dụ trên và lấy ra danh sách các chuyến bay bị hủy tới thành phố Atlanta, GA trong năm 2000
# Dữ liệu sắp theo theo ngày bay giảm dần.
flight_time_canceled = flight_time_df \
    .filter((col("CANCELLED") == 1) & 
            (col("DEST_CITY_NAME").contains('Atlanta, GA')) & 
            (year(col("FL_DATE")) == 2000)) \
    .select("DEST", "DEST_CITY_NAME", "FL_DATE", "ORIGIN", "OP_CARRIER", 
            "ORIGIN_CITY_NAME", "CANCELLED") \
    .orderBy(col("FL_DATE").desc())

flight_time_canceled.write \
.format("json") \
.mode("overwrite") \
.partitionBy("OP_CARRIER","ORIGIN") \
.option("path","/data/sink/json/") \
.option("maxRecordsPerFile", 10000) \
.save()


