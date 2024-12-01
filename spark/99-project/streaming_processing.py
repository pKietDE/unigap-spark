"""
    Module xử lý và ghi dữ liệu streaming từ Kafka vào PostgreSQL.

    Module này chứa lớp `StreamingJob` để đọc dữ liệu từ dòng Kafka, xử lý dữ liệu 
    sử dụng các User Defined Functions (UDFs), và ghi kết quả vào các bảng PostgreSQL 
    sử dụng lớp `ProductWriteProcessor`.

    Hàm `handle_write_report` chịu trách nhiệm ghi dữ liệu đã xử lý vào PostgreSQL 
    cho các báo cáo khác nhau dựa trên các lượt xem sản phẩm, quốc gia, trình duyệt, v.v.

    Module sử dụng API streaming có cấu trúc của PySpark để xử lý dữ liệu theo thời gian thực.
"""

import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from util.udf_manager import UDFManager
from top_view_analysis import ProductViewProcessor
from write_database import ProductWriteProcessor


def handle_write_report(batch_df, batch_id):
    """
        Xử lý và ghi kết quả dữ liệu streaming cho các báo cáo khác nhau vào PostgreSQL.

        Hàm này nhận một batch dữ liệu đã xử lý (`batch_df`), trích xuất các lượt xem 
        và chỉ số liên quan, và ghi chúng vào các bảng khác nhau trong PostgreSQL.

        Tham số:
        batch_df (DataFrame): Dữ liệu streaming đã xử lý cho batch hiện tại.
        batch_id (str): ID của batch đang được xử lý.
    """


    print(f"Processing Batch {batch_id}...")
    schema = "public"

    #Create Processor_view
    product_view_processor = ProductViewProcessor(batch_df)

    #Create Processor_write
    product_write_processor = ProductWriteProcessor()

    # Report 1
    view_product_today = product_view_processor.get_view_product_today()

    # Report 2
    view_country_today = product_view_processor.get_view_country_today()

    # Report 3
    view_referrer_today = product_view_processor.get_referrer_url_today()

    # Report 4
    store_view_in_country = product_view_processor.get_store_view_in_country()

    # Report 5 ID => 98370
    get_product_id_view_day = product_view_processor.get_product_id_view_day()

    # Report 6
    browser_os_view_to_hour = product_view_processor.get_browser_os_view_to_hour()


    # Write Report 1
    print("=== REPORT 1 ===")
    product_write_processor.write_postgres(view_product_today,"view_product_today",schema)

    # Write Report 2
    print("=== REPORT 2 ===")
    product_write_processor.write_postgres(view_country_today,"view_country_today",schema)

    # Write Report 3
    print("=== REPORT 3 ===")
    product_write_processor.write_postgres(view_referrer_today,"view_referrer_today",schema)

    # Write Report 4
    print("=== REPORT 4 ===")
    product_write_processor.write_postgres(store_view_in_country,"store_view_in_country",schema)

    # Write Report 5
    print("=== REPORT 5 ===")
    product_write_processor.write_postgres(get_product_id_view_day,"product_id_view_to_hour",schema)

    # Write Report 6
    print("=== REPORT 6 ===")
    product_write_processor.write_postgres(browser_os_view_to_hour,"browser_os_view_to_hour",schema)



class StreamingJob:
    """
        Lớp cho phép chạy công việc streaming đọc dữ liệu từ Kafka, xử lý dữ liệu 
        và ghi kết quả vào PostgreSQL.
    """


    def __init__(self, spark, schema, kafka_conf):
        """
            Khởi tạo đối tượng StreamingJob.

            Tham số:
            spark (SparkSession): Phiên làm việc Spark hiện tại.
            schema (StructType): Schema của dữ liệu.
            kafka_conf (dict): Cấu hình Kafka.
        """
        self.spark = spark
        self.schema = schema
        self.kafka_conf = kafka_conf

    def start(self):
        """
            Bắt đầu công việc streaming để đọc dữ liệu từ Kafka, xử lý và ghi kết quả 
            vào PostgreSQL.

            Phương thức này tạo các DataFrame cần thiết, áp dụng các biến đổi sử dụng UDFs, 
            và ghi dữ liệu đã xử lý vào PostgreSQL thông qua API `foreachBatch`.
    
        """


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
            .withColumn("country", get_country_udf(f.col("value.current_url"))) \
            .withColumn("time_stamp_convert", f.to_timestamp(f.from_unixtime(f.col("value.time_stamp")))) \
            .withColumn("local_time_convert", f.date_format(f.col("time_stamp_convert"), 'yyyy-MM-dd')) \
            .withColumn("hour", f.hour(f.col("time_stamp_convert"))) \
            .withColumn("day", f.day(f.col("time_stamp_convert"))) \
            .withColumn("month", f.month(f.col("time_stamp_convert"))) \
            .withColumn("browser", get_browser_udf(f.col("value.user_agent"))) \
            .withColumn("os", get_os_udf(f.col("value.user_agent"))) \

        df_product_view.printSchema()

        df_product_view = df_product_view.withWatermark("time_stamp_convert","10 minutes")

        # Use foreachBatch to write data into PostgreSQL
        query = df_product_view \
            .writeStream \
            .foreachBatch(handle_write_report) \
            .outputMode("append") \
            .trigger(processingTime='30 seconds')

        # Start the query and await termination
        streaming_query = query.start()

        # Wait for termination to keep the stream running
        streaming_query.awaitTermination()
