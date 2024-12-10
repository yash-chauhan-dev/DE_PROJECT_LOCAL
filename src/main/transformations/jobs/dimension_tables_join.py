from pyspark.sql.functions import *
from src.main.utility.logging_config import *
# enriching the data from different table


def join_dimension_tables(final_df_to_process,
                          customer_table_df, store_table_df, sales_team_table_df):

    # adding customer table
    # But i do not need all the columns so dropping it
    # save the result into s3_customer_df_join
    logger.info("Joining the final_df_to_process with customer_table_df ")
    s3_customer_df_join = final_df_to_process\
        .join(customer_table_df,
              ["customer_id"], "inner")\
        .drop("product_name", "price", "quantity", "additional_column",
              "customer_joining_date")

    s3_customer_df_join.printSchema()

    # adding store table details
    # But i do not need all the columns so dropping it
    # save the result into s3_customer_store_df_join

    logger.info("Joining the s3_customer_df_join with store_table_df ")
    s3_customer_store_df_join = s3_customer_df_join.join(store_table_df,
                                                         store_table_df["id"] == s3_customer_df_join["store_id"],
                                                         "inner")\
        .drop("id", "store_pincode", "store_opening_date", "reviews")

    # adding sales team table details
    # But i do not need all the columns so dropping it
    # save the result into s3_customer_store_sales_df_join

    logger.info(
        "Joining the s3_customer_store_df_join with sales_team_table_df ")
    s3_customer_store_sales_df_join = s3_customer_store_df_join.join(sales_team_table_df.alias("st"),
                                                                     col(
                                                                         "st.id") == s3_customer_store_df_join["sales_person_id"],
                                                                     "inner")\
        .withColumn("sales_person_first_name", col("st.first_name"))\
        .withColumn("sales_person_last_name", col("st.last_name"))\
        .withColumn("sales_person_address", col("st.address"))\
        .withColumn("sales_person_pincode", col("st.pincode"))

    s3_customer_store_sales_df_join = s3_customer_store_sales_df_join.drop(
        "id", "first_name", "last_name", "address", "pincode")

    return s3_customer_store_sales_df_join
