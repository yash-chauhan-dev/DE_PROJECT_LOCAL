import datetime
from email import header
from fileinput import filename
from os import error
import shutil
from threading import local
from urllib import response
from resources.dev import config
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.utility.encrypt_decrypt import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.spark_session import spark_session
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType
from pyspark.sql.functions import concat_ws, lit

# Keys to connect to AWS S3 Bucket
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

# Create S3 Client using the above encrypted keys
s3_client_provider = S3ClientProvider(
    decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

# List the available S3 buckets in AWS
response = s3_client.list_buckets()
print(response)
logger.info("List of Buckets: %s", response["Buckets"])


# Check if local directory already has a file
# if file is there then check if the same file is present in the staging area
# with status as A. If so then don't delete and try to rerun else give error.

csv_files = [file for file in os.listdir(
    config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f"""
        select distinct file_name from
        {config.database_name}.{config.product_staging_table}
        where file_name in ({str(total_csv_files)[1:-1]}) and status='I'
    """
    logger.info(f"dynamically created statement: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run was failed, please check!!!")
    else:
        logger.info("No record Found")
else:
    logger.info("last run was successfull!!!")


# Read files from s3 bucket and download to local directory
try:
    s3_reader = S3Reader()

    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(
        s3_client,
        config.bucket_name,
        folder_path=folder_path
    )
    logger.info("Absolute path on s3 bucket for csv file %s",
                s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No Data available to process")

except Exception as e:
    logger.error("Exited with error: %s", e)
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logging.info("File path available on s3 under %s bucket and folder name is %s",
             bucket_name, file_paths)

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)

except Exception as e:
    logger.error("File download error: %s", e)
    sys.exit()

# Get list of all files in the local directory
all_files = os.listdir(local_directory)
logger.info(
    f"List of files present at my local directory after download {all_files}")

# Filter files with .csv in their names amnd create absolute paths
if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(
                os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(
                os.path.join(local_directory, files)))

    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")

else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

logger.info("**************Listing the File******************")
logger.info("List of csv files that needs to be processed %s", csv_files)

logger.info("**************Creating Spark Session******************")

spark = spark_session()

logger.info("**************Spark Session Created******************")

# Check the required column in the schema of csv file
# If not required columns keep it in error files
# Else union all the data in one dataframes

logger.info(
    "**************Checking Schema for data loaded in s3******************")

correct_files = []

for data in csv_files:
    data_schema = spark.read.format("csv").options(
        header=True).load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing column for the {data}")
        correct_files.append(data)

logger.info(
    f"************ List of correct files **************{correct_files}")
logger.info(f"************ List of error files **************{error_files}")
logger.info(
    f"************ Moving Error Data to Error Directory if Any **************")

error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path, destination_path)
            logger.info(
                f"Moved {file_name} from s3 file path to {destination_path}")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(
                s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"'{file_path}' does not exist")
else:
    logger.info("*********** There is no error files available ****************")

# Additional files needs to be takeen care of
# Determine extra columns

# Before runningg process, staging table needs to be updated with ststus as Active(A) or Inactive(I)
logger.info(
    "********** Updating product staging table, that we have started the process ************")
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        statement = f"""
            INSERT INTO {db_name}.{config.product_staging_table}
            (file_name, file_location, created_date, status)
            VALUES ('{file_name}', '{file_name}', "{formatted_date}", 'A')
        """
        insert_statements.append(statement)
    logger.info(
        f"Insert statement created for staging table --- {insert_statements}")
    logger.info("************ Connecting with MySQL Server ***************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info(
        "****************** MySQL Server Connected Successfully *************")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("*************** There is no file to process *************")
    raise Exception(
        "************* No Data Available With Corrrect Files ************")

logger.info(
    "***************** Fixing Extra Column Coming From Source ***************")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

final_df_to_process = spark.createDataFrame([], schema=schema)

for data in correct_files:
    data_df = spark.read.format('csv').options(
        header=True, inferSchema=True).load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema)-set(config.mandatory_columns))
    logger.info(f"Extra columns present at source is {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn(
            "additional_column", concat_ws(",", *extra_columns)).select("customer_id", "store_id", "product_name",
                                                                        "sales_date", "sales_person_id", "price", "quantity", "total_cost", "additional_column")
        logger.info(f"Processed {data} and added 'additional_column'")
    else:
        data_df = data_df.withColumn(
            "additional_column", lit(None)).select("customer_id", "store_id", "product_name",
                                                   "sales_date", "sales_person_id", "price", "quantity", "total_cost", "additional_column")

    final_df_to_process = final_df_to_process.union(data_df)

logger.info(
    "*********** Final DataFrame from source which will be going to processing *******************")
print(final_df_to_process.count())
final_df_to_process.show()
