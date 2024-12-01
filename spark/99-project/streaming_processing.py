from util.udf_manager import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from top_view_analysis import ProductViewProcessor
from write_database import ProductWriteProcessor


def handle_write_report(batch_df, batch_id):
    print(f"Processing Batch {batch_id}...")
    schema = "public"
    
    #Create Processor_view
    product_view_processor = ProductViewProcessor(batch_df)

    #Create Processor_write
    product_write_processor = ProductWriteProcessor()

    # Report 1
    top10_view_product_today = product_view_processor.get_top10_view_product_today()

    # Report 2
    top10_view_country_today = product_view_processor.get_top10_view_country_today()

    # Report 3
    top5_referrer_url_today = product_view_processor.get_top5_referrer_url_today()

    # Report 4
    store_view_in_country = product_view_processor.get_store_view_in_country(country_code="de")

    # Report 5 ID => 98370
    product_id_view_to_hour = product_view_processor.get_product_id_view_to_hour(product_id="98370")

    # Report 6
    browser_os_view_to_hour = product_view_processor.get_browser_os_view_to_hour()


    # Write Report 1
    # print("=== REPORT 1 ===")
    # top10_view_country_today.show()

    # Write Report 4
    print("=== REPORT 4 ===")
    product_write_processor.write_postgres(store_view_in_country,"store_view_in_country",schema)

    # Write Report 5
    print("=== REPORT 5 ===")
    product_write_processor.write_postgres(product_id_view_to_hour,"product_id_view_to_hour",schema)

    # Write Report 6
    print("=== REPORT 6 ===")
    product_write_processor.write_postgres(browser_os_view_to_hour,"browser_os_view_to_hour",schema)



class StreamingJob:
    def __init__(self, spark, schema, kafka_conf):

        self.spark = spark
        self.schema = schema
        self.kafka_conf = kafka_conf

    def start(self):
        # Read kafka local
        df = self.spark.readStream \
            .format("kafka") \
            .options(**self.kafka_conf) \
            .load()

        

        # Register UDF
        udf_manager = UDFManager()
        get_country_udf = f.udf(udf_manager.get_country, StringType())
        get_browser_udf = f.udf(udf_manager.get_browser, StringType())
        get_os_udf = f.udf(udf_manager.get_os, StringType())

        # Get Value
        df_product_view = df.select(f.from_json(f.decode(f.col("value"), "UTF-8"), self.schema).alias("value"))

        df_product_view.printSchema()

        # Add Columns
        df_product_view = df_product_view \
            .withColumn("local_time_convert", f.to_date(f.substring(f.col("value.local_time"), 1, 10), 'yyyy-MM-dd')) \
            .withColumn("country", get_country_udf(f.col("value.current_url"))) \
            .withColumn("time_stamp_convert", f.to_timestamp(f.from_unixtime(f.col("value.time_stamp")))) \
            .withColumn("hour", f.hour(f.col("time_stamp_convert"))) \
            .withColumn("browser", get_browser_udf(f.col("value.user_agent"))) \
            .withColumn("os", get_os_udf(f.col("value.user_agent"))) \

        df_product_view.printSchema()
            
        df_product_view = df_product_view.withWatermark("time_stamp_convert","10 minutes")

        
        # Use foreachBatch to write data into PostgreSQL
        query = df_product_view \
            .writeStream \
            .foreachBatch(handle_write_report) \
            .outputMode("append") \
            .trigger(processingTime='30 seconds')  # Set trigger interval


        # Start the query and await termination
        streaming_query = query.start()

        # Wait for termination to keep the stream running
        streaming_query.awaitTermination()
