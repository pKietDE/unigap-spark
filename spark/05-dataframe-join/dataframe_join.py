from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("05-dataframe-join").master("spark://spark:7077").getOrCreate()

df1 = spark.read \
    .format("json") \
    .option("inferSchema","true") \
    .load("/data/dataframe-join/d1/*")
df2 = spark.read \
    .format("json") \
    .option("inferSchema","true") \
    .load("/data/dataframe-join/d2/*")

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


#Viết chương trình lấy ra danh sách các chuyến bay bị hủy tới thành phố Atlanta, GA trong năm 2000
#Dữ liệu sắp theo theo ngày bay giảm dần.
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
df1.join(df2, on = "id", how = "left").filter(col("CANCELLED") == 1) \
    .select("id","DEST","DEST_CITY_NAME","FL_DATE","ORIGIN","ORIGIN_CITY_NAME","CANCELLED") \
    .orderBy(col("FL_DATE").desc()).show()




#Viết chương trình lấy ra danh sách các destination, năm và tổng số chuyến bay bị hủy của năm đó.
#Dữ liệu sắp xếp theo mã destination và theo năm.
df1.join(df2, on = "id", how = "left").filter((col("CANCELLED") == 1) & (year(to_date("FL_DATE", "d/M/yyyy")).isNotNull())) \
    .groupBy("DEST",year(to_date("FL_DATE", "d/M/yyyy")).alias("FL_YEAR")).count() \
    .withColumnRenamed("count","NUM_CANCELLED_FLIGHT") \
    .orderBy(col("DEST"),col("FL_YEAR")) \
    .show()

