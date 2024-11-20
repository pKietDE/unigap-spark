from gender_util.gender_util import parse_gender 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def handle_no_employees(input_str):
    if "500-1000" in input_str:
        return True
    elif "More than 1000" in input_str:
        return True
    else :
        return False
    
if __name__ == "__main__":

    # init spark
    spark = SparkSession.builder.appName("udf").master("spark://spark:7077").getOrCreate()

    # Read file csv
    survey_df = spark.read.option("header","true").option("inferSchema","true").csv("/data/udf/survey.csv")


    #register functions parse_gender
    spark.udf.register("parse_gender_udf",parse_gender,StringType())
    survey_df = survey_df.withColumn("Gender",expr("parse_gender_udf(Gender)"))

    #register functions handle_no_employees
    spark.udf.register("is_more_than_500_no_employess_udf",handle_no_employees,BooleanType())
    is_more_than_500_no_employess_udf = udf(lambda input_str : handle_no_employees(input_str),BooleanType())

    # Create Table
    survey_df.createOrReplaceTempView("my_table")

    #method 1
    survey_df.filter(is_more_than_500_no_employess_udf(col("no_employees")) == True).select("Age","Gender","Country","state","no_employees").show()

    #method 2
    spark.sql("SELECT Age,Gender,Country,state,no_employees FROM my_table WHERE is_more_than_500_no_employess_udf(no_employees)").show()
