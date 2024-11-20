from pyspark.sql import SparkSession
from pyspark.sql.window import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("windown_function").master("spark://spark:7077").getOrCreate()
summary_df = spark.read.parquet("/data/window-function/summary.parquet")

windowSpec = Window.partitionBy("country").orderBy(col("InvoiceValue").desc())

summary_df \
    .withColumn("rank",rank().over(windowSpec)) \
    .orderBy(col("country"),col("rank")) \
    .show()


windowSpec2 = Window.partitionBy("country").orderBy(col("WeekNumber")).rowsBetween(Window.unboundedPreceding,Window.currentRow)
windowSpec3 = Window.partitionBy("Country").orderBy(col("WeekNumber"))

summary_df \
    .withColumn("PercentGrowth",round(((col("InvoiceValue") - lag("InvoiceValue",1).over(windowSpec3)) / lag("InvoiceValue",1).over(windowSpec3))*100,2)) \
    .withColumn("AccumulateValue",round(sum("InvoiceValue").over(windowSpec2),2)) \
    .fillna({"PercentGrowth":0.0}) \
    .orderBy(col("country"),col("WeekNumber")) \
    .show()

