from pyspark.sql.functions import col
from general_usage import measure_time

@measure_time
def build_query_for_Part_Pruning(df_1, df_2, key_1, key_2, *selec_col, order=None, perform_action=True, **filters):
    joined_df = df_1 \
        .join(df_2, col(f"{key_1}") == col(f"{key_2}")) 
    for field_name, value in filters.items():
        joined_df = joined_df.filter(col(field_name) == value) 
    if order:
        joined_df = joined_df.orderBy(order)
    if selec_col:
        joined_df = joined_df.select(*selec_col)
    if perform_action:
        joined_df.write.format("noop").mode("overwrite").save()

    return joined_df

    


# joined_df.write.format("noop").mode("overwrite").save()
# --  Without this, @measure_time decorator affects the calculation of the time required by the execution plan.
# Since Spark is characterized by lazy evaluation action is required to count time for pical execution


