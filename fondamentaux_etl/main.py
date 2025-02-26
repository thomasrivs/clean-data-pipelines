from pyspark.sql import SparkSession
from pyspark.sql.functions import sum


# 1 --> Exctract : Read csv
def extract(spark: SparkSession, input_file_path: str):
    """Extract data from any csv source"""
    try:
        return spark.read.csv(
            input_file_path, 
            header=True, 
            inferSchema=True
            )
    except Exception as e:
        print(f"Error {e}")


# 2 --> Tranform : Calcule le total des ventes par produit
def transform(df):
    """Compute total unit sold for a product"""
    try:
        return df.groupBy("product_id").agg(sum("quantity").alias('total_unit_sold'))
    except Exception as e:
        print(f"Error {e}")

# 3 --> Output en parquet
def load(df, outpout_file_path: str):
    """Load data into  parquet file"""
    df.write.mode("overwrite") \
    .parquet(
        outpout_file_path, 
        compression="snappy"
        )

def main():
    spark = SparkSession \
    .builder \
    .appName("etl") \
    .getOrCreate()

    try:
        input_file_path= "/Users/thomasrivieres/clean-data-pipelines/.data/sales.csv"
        outpout_file_path= "/Users/thomasrivieres/clean-data-pipelines/.data/result.parquet"

        raw_df = extract(spark, input_file_path)
        df_transformed = transform(raw_df)
        load(df_transformed, outpout_file_path)

    except Exception as e:
        print(f"Error {e}")



if __name__ == "__main__":
    main()    