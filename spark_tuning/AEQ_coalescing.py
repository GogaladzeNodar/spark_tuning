from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor
import time

spark = SparkSession.builder \
.appName("coalescing") \
.master("local[*]") \
.config("spark.driver.memory", "4g") \
.config("spark.sql.adaptive.enabled", "true") \
.config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
.config("spark.sql.adaptive.skewJoin.enabled", "false") \
.config("spark.sql.adaptive.localShuffleReader.enabled", "false") \
.config("spark.sql.shuffle.partitions", "200") \
.getOrCreate()


customers_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/csv/customers.csv") \
    .alias("cust")


accounts_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/csv/accounts.csv") \
    .alias("acc")


cards_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/csv/cards.csv") \
    .alias("card")


customers_accounts_cards_df = (
customers_df.join(accounts_df, ["customer_id"], "left") \
.join(cards_df, "account_id") \
.withColumn("round_credit", floor(col("credit_score") / 100) * 100)
)

print(f"partitions before actions {customers_accounts_cards_df.rdd.getNumPartitions()}")
customers_accounts_cards_df.cache() # we have two action so cache this dataframe So that this operation is not performed twice.

total_rows = customers_accounts_cards_df.count()
print(f"Total rows: {total_rows}")


customers_accounts_cards_df = customers_accounts_cards_df.repartition(200, "customer_id")

print(f"partitions after actions {customers_accounts_cards_df.rdd.getNumPartitions()}")

# customers_accounts_cards_df.write \
# .mode("overwrite") \
# .partitionBy("round_credit") \
# .parquet("data/partitioned_data/customers_accounts_cards.parquet")

result_df = (
    customers_accounts_cards_df.groupBy("round_credit")
    .count()
)

result_df.show()


print(f"partitions after actions {result_df.rdd.getNumPartitions()}")

# partitions before actions 2
# Total rows: 100000
# partitions after actions 2    

# let's make many partitions manually 200 partitions
# and see how many partitions we have after the action. 

time.sleep(1000)