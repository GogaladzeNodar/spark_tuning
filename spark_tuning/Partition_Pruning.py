from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from spark_general import  build_query_for_Part_Pruning



spark = SparkSession.builder \
    .appName("MySparkApp") \
    .master("local[*]") \
    .getOrCreate()


accounts_df_1 = spark.read.csv('data/csv/accounts.csv', header=True, inferSchema=True)

customers_df = spark.read.csv('data/csv/customers.csv', header=True, inferSchema=True)

accounts_df_1.write \
    .mode("overwrite") \
    .partitionBy("account_type") \
    .parquet("data/partitioned_data/accounts.parquet")



accounts_df = spark.read.parquet("data/partitioned_data/accounts.parquet")



joined_df, used_time = build_query_for_Part_Pruning(
    customers_df.alias("cust"),  
    accounts_df.alias("acc"),    
    "cust.customer_id",          
    "acc.customer_id",           
    col("cust.customer_id"),
    col("cust.first_name"),
    col("cust.last_name"),
    col("acc.account_id"),
    col("acc.balance_usd"),
    order=None, 
    perform_action=True,              
    **{"acc.account_type": "Checking"}  
)

print(f"time used for query is - {used_time}")

joined_df.show(5)


#joined_df.explain(True)


# Initially, I tried to partition accounts dataframe by customer_id to used partition pruning.
# However, since the data contains many unique customer_id values
# it needs to create many partitions, time to solve this job was Approximately 5 min 
# while  partition by account_type needs 3 second. (it creates 3-4 partition and whole data is not big)




#----------------------------------------------------------------------------------------------------------------------

# now let's see how time changes when data is not partitioned

joined_df_2, time_2 = build_query_for_Part_Pruning(
    customers_df.alias("cust"),  
    accounts_df_1.alias("acc"),    
    "cust.customer_id",          
    "acc.customer_id",           
    col("cust.customer_id"),
    col("cust.first_name"),
    col("cust.last_name"),
    col("acc.account_id"),
    col("acc.balance_usd"),
    order=None,     
    perform_action=True,          
    **{"acc.account_type": "Checking"}  
) 

print(f"time used for query is - {time_2}")

joined_df_2.show(5)


# this is result of two action
# time used for query is - 0.3796069622039795                                     
# +---------------+----------+---------+---------------+-----------+
# |    customer_id|first_name|last_name|     account_id|balance_usd|
# +---------------+----------+---------+---------------+-----------+
# |CUSOC6UZHR5XFF0|    Ashley|     Wang|ACC3QVX533EWB9W|   13823.83|
# |CUSVQJT7YE511MK|     David|  Cabrera|ACCSIWX86TAHYPZ|   62198.93|
# |CUSAZZ81SDW4SHN|    Alicia|   Rivera|ACCPNS9Q3QCEC8Z|    3642.73|
# |CUS039TEX8FSV87|    Joshua|  Andrade|ACCC5YLJ5HEGV93|  106100.84|
# |CUS039TEX8FSV87|    Joshua|  Andrade|ACCQIEHTB9UED41|   17102.26|
# +---------------+----------+---------+---------------+-----------+
# only showing top 5 rows
# time used for query is - 0.2640540599822998
# +---------------+----------+----------+---------------+-----------+
# |    customer_id|first_name| last_name|     account_id|balance_usd|
# +---------------+----------+----------+---------------+-----------+
# |CUSAEOACKBH8CK6|    Pamela|Washington|ACCOD48PUCAEHKH|  182700.46|
# |CUSBQNS6ND3E3D8|    Sandra|Fitzgerald|ACCEG7CN1W6R987|    52092.2|
# |CUSA9P9URVI8PJ6|  Patricia|   Johnson|ACC44HPKAAOS2L0|  164330.33|
# |CUS3WZN1J3OBFIV|     David|     Payne|ACCLHRDEDFIU9S4|  194333.08|
# |CUSK7K0H4F3JWB2|   Gabriel|    Hodges|ACCW9N21DU90SVJ|   67126.79|
# +---------------+----------+----------+---------------+-----------+

