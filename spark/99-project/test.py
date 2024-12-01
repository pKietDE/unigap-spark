from pyspark.sql import SparkSession
from util.config import *
from pyspark.sql.types import *
from schemas import SchemaManager
from streaming_processing import StreamingJob


if __name__ == "__main__":
    conf = Config()


    spark_conf = conf._get_spark_conf()
    kafka_conf = conf._get_section_conf("KAFKA")

    # Get Schema
    schema_manager = SchemaManager()
    schema = schema_manager.get_schema()

    # init spark
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    streaming_job = StreamingJob(spark,schema,kafka_conf)

    streaming_job.start()