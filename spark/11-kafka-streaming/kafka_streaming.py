import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType

from util.config import Config

if __name__ == "__main__":
    conf = Config()
    spark_conf = conf.spark_conf
    kafka_conf = conf.kafka_conf

    schema = StructType([
        StructField("_id", StringType(), True),
        StructField("time_stamp", LongType(), True),
        StructField("ip", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("resolution", StringType(), True),
        StructField("user_id_db", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("api_version", StringType(), True),  
        StructField("store_id", StringType(), True), 
        StructField("local_time", StringType(), True),  
        StructField("show_recommendation", StringType(), True),
        StructField("current_url", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("utm_source", StringType(), True),
        StructField("utm_medium", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("product_id", StringType(), True), 
        StructField("price", StringType(), True), 
        StructField("currency", StringType(), True), 
        StructField("order_id", StringType(), True), 
        StructField("is_paypal", StringType(), True), 
        StructField("viewing_product_id", StringType(), True), 
        StructField("option", ArrayType(
            StructType([
                StructField("option_label", StringType(), True),
                StructField("option_id", StringType(), True),
                StructField("value_label", StringType(), True),
                StructField("value_id", StringType(), True)
            ])
        ), True),
        StructField("cat_id", StringType(), True), 
        StructField("collect_id", StringType(), True), 

    ])

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()

    df.printSchema()

    query = df.select(f.from_json(f.decode(col("value"), "UTF-8"), schema).alias("value")) \
        .select("value.*") \
        .writeStream \
        .format("console") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()

    spark.stop()