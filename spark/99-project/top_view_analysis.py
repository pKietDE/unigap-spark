from datetime import datetime
import pyspark.sql.functions as f

class ProductViewProcessor:
    def __init__(self, df):
        self.df = df
        self.date_today = datetime.today().strftime("%Y-%m-%d")

    def get_top10_view_product_today(self):
        return self.df \
            .filter(f.col("local_time_convert") == self.date_today) \
            .groupBy(f.col("value.product_id")) \
            .count() \
            .withColumnRenamed("count", "view_product_today") \
            .orderBy(f.desc("view_product_today")) \
            .limit(10)

    def get_top10_view_country_today(self):
        return self.df \
            .filter(f.col("local_time_convert") == self.date_today) \
            .groupBy(f.col("country")) \
            .count() \
            .withColumnRenamed("count", "view_country_today") \
            .orderBy(f.desc("view_country_today")) \
            .limit(10)

    def get_top5_referrer_url_today(self):
        return self.df \
            .filter((f.col("local_time_convert") == self.date_today) & (~f.col("value.referrer_url").isNull())) \
            .groupBy(f.col("value.referrer_url")) \
            .count() \
            .withColumnRenamed("count", "view_referrer_url_today") \
            .orderBy(f.desc("view_referrer_url_today")) \
            .limit(5)

    def get_store_view_in_country(self, country_code : str):
        return self.df \
            .filter((f.col("country") == country_code)) \
            .groupBy(f.col("country"), f.col("value.store_id")) \
            .count() \
            .withColumnRenamed("count", "view_store") \
            .orderBy(f.desc("view_store"))

    def get_product_id_view_to_hour(self, product_id: str):
        return self.df \
            .filter((f.col("value.product_id") == product_id)) \
            .groupBy(f.col("value.product_id"),f.col("hour")) \
            .count() \
            .withColumnRenamed("count", "view_product_id_to_hour") \
            .orderBy(f.asc("hour"))
    
    def get_browser_os_view_to_hour(self):
        return self.df \
            .groupBy(f.col("browser"),f.col("os"),f.col("hour")) \
            .count() \
            .withColumnRenamed("count", "view_browser_os_to_hour") \
            .orderBy(f.asc("hour"),f.desc("view_browser_os_to_hour"))
