from pyspark.sql import SparkSession

if __name__ == "__main__":
        spark = SparkSession.builder \
        .appName("spark-sql") \
        .master("spark://spark:7077") \
        .getOrCreate()

        df1 = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/data/spark-sql/survey.csv")


        df1.printSchema()


        """Viết chương trình sử dụng Spark SQL lấy ra danh sách các quốc gia và số người là nam có độ tuổi < 40.

        Một người là nam thì trường Gender sẽ có giá trị là male hoặc m (lưu ý không phân biệt viết hoa/thường).

        Dữ liệu sắp xếp theo số người tăng dần. Nếu số người bằng nhau thì sắp xếp theo tên quốc gia."""

        df1.createOrReplaceTempView("survey")
        query_male_of_country = """
                SELECT country , count(*) as num_male
                FROM survey
                WHERE lower(gender) In ("male","man","m") and age < 40
                GROUP BY country
                ORDER BY num_male,country
        """
        spark.sql(query_male_of_country).show()



        """
        Viết chương trình sử dụng Spark SQL lấy ra danh sách quốc gia và số nam, nữ của từng quốc gia.

        Một người là nam thì trường Gender sẽ có giá trị là male hoặc man hoặc m (lưu ý không phân biệt viết hoa/thường).

        Một người là nữ thì trường Gender sẽ có giá trị là female hoặc woman hoặc w (lưu ý không phân biệt viết hoa/thường).

        Dữ liệu sắp xếp theo tên quốc gia.
        """

        query_male_and_female_of_country = """
                SELECT country 
                , SUM(CASE WHEN lower(gender) In ("male","man","m") THEN 1 else 0 END) as num_male
                , SUM(CASE WHEN lower(gender) In ("female","woman","w") THEN 1 else 0 END) as num_female
                FROM survey
                GROUP BY country
                ORDER BY country
        """
        spark.sql(query_male_and_female_of_country).show()
