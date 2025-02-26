from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession \
    .builder \
    .appName("etl") \
    .getOrCreate()

# 1 --> Exctract : Read csv
df_sales = spark.read.csv('/Users/thomasrivieres/clean-data-pipelines/.data/sales.csv', 
                          header=True, inferSchema=True)

# 2 --> Tranfoorm : Calcule le total des ventes par produit
result_df = df_sales.groupBy("product_id").agg(sum("quantity").alias('total_unit_sold'))

# 3 --> Output en parquet
result_df.write.parquet("/Users/thomasrivieres/clean-data-pipelines/.data/result.parquet", compression="snappy")

df_parquet = spark.read.parquet("/Users/thomasrivieres/clean-data-pipelines/.data/result.parquet")
df_parquet.show()



if __name__ == "__main__":
    pass
    