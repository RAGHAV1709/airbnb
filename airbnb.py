print("Default number of partitions : ",spark.sparkContext.defaultParallelism)

# 1. Read both tables from Hive and store them in different dataframes
df_orders = spark.table("tables_by_spark.orders_pq")
df_customers = spark.table("tables_by_spark.customers_pq")

print(f'Number of partitions in df_orders : {df_orders.rdd.getNumPartitions()}')
print(f'Number of partitions in df_customers : {df_customers.rdd.getNumPartitions()}')

print('Read orders and customers data successfully')


# 3. Filter records where order_status='delivered' in orders dataframe
df_orders_delivered = df_orders.filter(df_orders.order_status == 'delivered')
print(f'Number of partitions in df_orders_delivered : {df_orders_delivered.rdd.getNumPartitions()}')

# 4. Perform groupby operation on customer_id column to calculate number of orders delivered to each customer
df_orders_grouped = df_orders_delivered.groupBy("customer_id").count()
print(f'Number of partitions in df_orders_grouped : {df_orders_grouped.rdd.getNumPartitions()}')

# 5. Do a left join of customers dataframe with df_orders_grouped on customer_id column
df_joined = df_customers.join(df_orders_grouped, on="customer_id", how="left")
print(f'Number of partitions in df_joined : {df_joined.rdd.getNumPartitions()}')

print('Join completed')

# 6. Show some records (First Action - triggers first job with multiple stages)
df_joined.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('/tmp/spark_output/final_result')

# Optional - IF you want to write data in any Hive table
# df_joined.write.mode("append").insertInto("<database_name>.<table_name>")

print("Write Successfull")