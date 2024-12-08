from urllib import response
from resources.dev import config
from src.main.utility.encrypt_decrypt import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *

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
        where file_name in ({str(total_csv_files)[1:-1]}) and status='I
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
