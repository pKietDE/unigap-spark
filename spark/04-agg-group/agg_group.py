from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("agg_group").master("spark://spark:7077").getOrCreate()

agg_df = spark.read.option("header","true").option("inferSchema","true").csv("/data/agg-group/invoices.csv")


agg_df.printSchema()


# Xu ly invoice date
agg_df = agg_df.withColumn("Year", year(to_date(split(col("InvoiceDate")," ").getItem(0),"dd-MM-yyyy")))

# Viết chương trình lấy ra danh sách các quốc gia, năm, số hóa đơn, số lượng sản phẩm, tổng sô tiền của từng quốc gia và năm đó
# Dữ liệu sắp xếp theo tên quốc gia và theo năm.
agg_df.groupBy("Country","Year").agg(
    count("*").alias("num_invoices")
    ,sum(col("quantity")).alias("total_quantity")
    ,sum(col("Quantity") * col("UnitPrice")).alias("invoice_value")).orderBy("Country","Year").show()



#Viết chương trình lấy ra top 10 khách hàng có số tiền mua hàng nhiều nhất trong năm 2010
#Dữ liệu sắp xếp theo số tiền giảm dần, nếu số tiền bằng nhau thì sắp xếp theo mã khách hàng tăng dần
agg_df.filter((col("year") == 2010) & (col("CustomerID").isNotNull())) \
      .groupBy("CustomerID") \
      .agg(round(sum(col("Quantity") * col("UnitPrice")), 2).alias("invoice_value")) \
      .orderBy(col("invoice_value").desc(), col("CustomerID")) \
      .limit(10) \
      .show()