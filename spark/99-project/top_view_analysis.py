"""
Module để xử lý dữ liệu về lượt xem sản phẩm, cửa hàng, quốc gia, trình duyệt, hệ điều hành,
và các thông tin khác liên quan đến lượt xem sản phẩm.
"""

from datetime import datetime
import pyspark.sql.functions as f


class ProductViewProcessor:
    """
    Lớp xử lý dữ liệu về lượt xem sản phẩm.

    Các phương thức trong lớp này giúp xử lý dữ liệu từ DataFrame, tạo bảng sự kiện (fact view)
    và cung cấp các báo cáo về lượt xem theo các nhóm như sản phẩm, quốc gia, cửa hàng, trình duyệt, hệ điều hành.

    Các phương thức bao gồm:
        - create_fact_view: Tạo bảng sự kiện từ dữ liệu thô.
        - get_view_product_today: Tính tổng số lượt xem sản phẩm trong ngày hôm nay.
        - get_view_country_today: Tính tổng số lượt xem theo quốc gia trong ngày hôm nay.
        - get_referrer_url_today: Tính tổng số lượt xem theo URL giới thiệu trong ngày hôm nay.
        - get_store_view_in_country: Tính tổng số lượt xem theo cửa hàng trong một quốc gia.
        - get_browser_os_view_to_hour: Tính tổng số lượt xem theo trình duyệt và hệ điều hành theo giờ.
    """
    @staticmethod
    def create_fact_view(df):
        """
        Tạo bảng sự kiện với lượt xem được tổng hợp.
        
        :param df: DataFrame đầu vào
        :return: DataFrame đã tổng hợp
        """
        return df.groupBy(
                f.col("value.product_id").alias("product_id"),
                f.col("value.store_id").alias("store_id"), 
                f.col("local_time_convert"),
                f.col("month"),
                f.col("day"),
                f.col("hour"),
                f.col("browser"),
                f.col("os"),
                f.col("country"),
                f.col("value.referrer_url").alias("referrer"),
                f.window(f.col("time_stamp_convert"), "1 hour").alias("time_window")
            ).agg(
                f.count("*").alias("view_count")
            ).withColumn(
                "time_stamp", f.col("time_window.start")
            ).select(
               "*"
            )

    def __init__(self, df):
        """
        Khởi tạo một đối tượng với dữ liệu `df` và xử lý dữ liệu để tạo bảng sự kiện (fact view).
        
        :param df: DataFrame chứa dữ liệu sản phẩm cần xử lý.

        :return: None
        """
        self.fact_view = ProductViewProcessor.create_fact_view(df)
        self.date_today = datetime.today().strftime("%Y-%m-%d")

        print("====Fact View Schema...  =====")
        self.fact_view.printSchema()

    def get_view_product_today(self):
        """ Tính tổng số lượt xem sản phẩm trong ngày hôm nay. """
        return self.fact_view \
            .filter(~f.col("product_id").isNull()) \
            .groupBy(f.col("product_id")) \
            .agg(f.sum("view_count").alias("view_product_today"))

    def get_view_country_today(self):
        """ Tính tổng số lượt xem theo quốc gia trong ngày hôm nay. """
        return self.fact_view \
            .groupBy(f.col("country")) \
            .agg(f.sum("view_count").alias("view_country_today"))

    def get_referrer_url_today(self):
        """ Tính tổng số lượt xem theo URL giới thiệu trong ngày hôm nay. """
        return self.fact_view \
            .groupBy(f.col("referrer")) \
            .agg(f.sum("view_count").alias("view_referrer_today"))

    def get_store_view_in_country(self):
        """
        Tính tổng số lượt xem theo cửa hàng trong một quốc gia.

        :return: DataFrame chứa thông tin về quốc gia, mã cửa hàng và tổng số lượt xem
        cho từng cửa hàng trong quốc gia được chỉ định.
        """
        return self.fact_view \
            .groupBy(f.col("country"), f.col("store_id")) \
            .agg(f.sum("view_count").alias("view_store_in_country")) \

    def get_product_id_view_day(self):
        """
        Tính tổng số lượt xem của id sản phẩm trong ngày.

        :return: DataFrame chứa thông tin về Id sản phẩm và tổng số lượt xem
        theo giờ trong ngày.
        """

        return self.fact_view \
            .filter(~f.col("product_id").isNull()) \
            .groupBy(f.col("product_id"),f.col("month"),f.col("day"),f.col("hour")) \
            .agg(f.sum("view_count").alias("view_product_id_in_day")) \
            .orderBy(f.desc("view_product_id_in_day"))


    def get_browser_os_view_to_hour(self):
        """ Tính tổng số lượt xem theo trình duyệt, hệ điều hành và theo giờ. """
        return self.fact_view \
            .groupBy(f.col("browser"), f.col("os"), f.col("hour"), f.col("local_time_convert")) \
            .agg(f.sum("view_count").alias("view_browser_os_to_hour"))
