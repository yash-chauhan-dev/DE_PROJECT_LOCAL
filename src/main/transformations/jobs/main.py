from threading import local
from urllib import response
from resources.dev import config
from src.main.download.aws_file_download import S3FileDownloader
from src.main.read.aws_read import S3Reader
from src.main.utility.encrypt_decrypt import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.spark_session import spark_session

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
