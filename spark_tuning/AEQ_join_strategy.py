
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, when
import time


spark = SparkSession.builder \
    .appName("AEQ-TEST") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.logLevel", "INFO") \
    .getOrCreate()


spark.sparkContext.setLogLevel("INFO")
#-----------------------------------------------------------------------------------------------------------------------

# accounts_parquet_path = "data/partitioned_data/accounts.parquet"

# accounts_df = spark.read.parquet(accounts_parquet_path)

# # data is really small, to use adaptive execution query lets make this files bigger

# replication_df = spark.range(0, 25000).withColumnRenamed("id", "replica_id")



# new_accounts_df = accounts_df.crossJoin(replication_df)

# skewd_df = new_accounts_df.withColumn(
#     "account_type", 
#     when(col("replica_id") % 2 == 0, "Buisness").otherwise(col("account_type"))
#     )


skewd_data = "data/big_data/skew_data.parquet"

# skewd_df.write \
#     .mode("overwrite") \
#     .partitionBy("account_type") \
#     .parquet(skewd_data)
                
#-------------------------------------uncomment this part to generate huge parquet




# now let's see what happens when we group data and how how AEQ helps in runtime of the query

skewed_df = spark.read.parquet(skewd_data)

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

(spark.read
.option("header", "true")
.option("inferSchema", "true") 
.csv("data/csv/customers.csv")
.write
.mode("overwrite")
.parquet("data/partitioned_data/customers.parquet")
)


customers_df = spark.read.parquet("data/partitioned_data/customers.parquet")

# joined_df = skewed_df.join(
#     customers_df, skewed_df.customer_id == customers_df.customer_id,
#     "inner"
# )
# joined_df.count()

# joined_df.explain("formatted")

# at this point we don't use AEQ, customers data is alredty small and it is brodcasted by catlyins optimizer in planing,
# now make customers data bigger, Let's trick the Catalyst Optimizer ანდ see if  AEQ changes join strategy





customers_replica_df = spark.range(0, 40)

customers_df = customers_df.crossJoin(customers_replica_df)

joined_df = skewed_df.join(
    customers_df, skewed_df.customer_id == customers_df.customer_id,
    "inner"
) \
.filter(customers_df.credit_score > 500)
joined_df.count()

joined_df.explain("formatted")


time.sleep(1000)

# now finall plan use brodcastjoin 

# == Physical Plan ==
# AdaptiveSparkPlan (42)
# +- == Final Plan ==
#    ResultQueryStage (25), Statistics(sizeInBytes=8.0 EiB)
#    +- * HashAggregate (24)
#       +- ShuffleQueryStage (23), Statistics(sizeInBytes=144.0 B, rowCount=9)
#          +- Exchange (22)
#             +- * HashAggregate (21)
#                +- * Project (20)
#                   +- * BroadcastHashJoin Inner BuildRight (19)
#                      :- AQEShuffleRead (7)
#                      :  +- ShuffleQueryStage (6), Statistics(sizeInBytes=55.9 GiB, rowCount=1.88E+9)
#                      :     +- Exchange (5)
#                      :        +- * Project (4)
#                      :           +- * Filter (3)
#                      :              +- * ColumnarToRow (2)
#                      :                 +- Scan parquet  (1)
#                      +- BroadcastQueryStage (18), Statistics(sizeInBytes=144.0 MiB, rowCount=1.27E+6)
#                         +- BroadcastExchange (17)
#                            +- * BroadcastNestedLoopJoin Cross BuildRight (16)
#                               :- * Project (11)
#                               :  +- * Filter (10)
#                               :     +- * ColumnarToRow (9)
#                               :        +- Scan parquet  (8)
#                               +- BroadcastQueryStage (15), Statistics(sizeInBytes=0.0 B, rowCount=40)
#                                  +- BroadcastExchange (14)
#                                     +- * Project (13)
#                                        +- * Range (12)