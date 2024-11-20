from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, when, col, sum


if __name__ == "__main__":
        spark = SparkSession.builder \
        .appName("spark-sql") \
        .master("spark://spark:7077") \
        .getOrCreate()

        df1 = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/data/dataframe-api/survey.csv")


        df1.show(2)
        df1.printSchema()


        """Viết chương trình sử dụng Spark SQL lấy ra danh sách các quốc gia và số người là nam có độ tuổi < 40.

        Một người là nam thì trường Gender sẽ có giá trị là male hoặc m (lưu ý không phân biệt viết hoa/thường).

        Dữ liệu sắp xếp theo số người tăng dần. Nếu số người bằng nhau thì sắp xếp theo tên quốc gia."""

        df1.filter(col("age") < 40).groupBy("country").agg(
            sum(when(lower(col("gender")).isin(["male", "man", "m"]), 1).otherwise(0)).alias("num_male")
        ).orderBy(col("num_male"), col("country")).show()


        """
        Viết chương trình sử dụng Spark SQL lấy ra danh sách quốc gia và số nam, nữ của từng quốc gia.

        Một người là nam thì trường Gender sẽ có giá trị là male hoặc man hoặc m (lưu ý không phân biệt viết hoa/thường).

        Một người là nữ thì trường Gender sẽ có giá trị là female hoặc woman hoặc w (lưu ý không phân biệt viết hoa/thường).

        Dữ liệu sắp xếp theo tên quốc gia.
        """

        df1.groupBy("country").agg(
            sum(when(lower(col("gender")).isin(["male", "man", "m"]), 1).otherwise(0)).alias("num_male"),
            sum(when(lower(col("gender")).isin(["female", "woman", "w"]), 1).otherwise(0)).alias("num_female")
        ).orderBy("country").show() 
