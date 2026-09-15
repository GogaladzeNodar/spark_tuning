from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import time



spark = SparkSession.builder \
    .appName("MySparkApp") \
    .master("local[*]") \
    .getOrCreate()

time0 = time.time()

accounts_df = spark.read.csv('data/csv/accounts.csv', header=True, inferSchema=True)

customers_df = spark.read.csv('data/csv/customers.csv', header=True, inferSchema=True)

accounts_df.write \
    .mode("overwrite") \
    .partitionBy("account_type") \
    .parquet("data/partitioned_data/accounts.parquet")



accounts_df = spark.read.parquet("data/partitioned_data/accounts.parquet")


joined_df = customers_df.alias("cust") \
    .join(accounts_df.alias("acc"), col("cust.customer_id") == col("acc.customer_id")) \
    .where(col("acc.account_type") == "Checking") \
    .select(
        col("cust.customer_id"),
        col("cust.first_name"),
        col("cust.last_name"),
        col("acc.account_id"),
        col("acc.balance_usd")
    ) 

joined_df.show(5)

spent_time = time.time() - time0

print(f"spent_time = {spent_time}")

joined_df.explain(True)


# Initially, I tried to partition accounts dataframe by customer_id to used partition pruning.
# However, since the data contains many unique values
# it needs to create many partitions, time to solve this job was Approximately 5 min 
# while  partition by account_type needs 3 second. (it creates only 3 or for partition and whole data is not big)
