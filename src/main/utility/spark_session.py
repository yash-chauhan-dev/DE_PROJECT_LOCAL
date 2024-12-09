from src.main.utility.logging_config import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql import SparkSession
import findspark
findspark.init()


def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("retail_data_processing")\
        .config("spark.driver.extraClassPath", "C:\\mysql_jar\\mysql-connector-j-9.1.0\\mysql-connector-j-9.1.0.jar") \
        .getOrCreate()
    logger.info("spark session %s", spark)
    return spark
