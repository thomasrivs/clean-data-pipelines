from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
import datetime


class BasePipeline:
    def extract(self):
        pass

    def transform(self, df):
        pass
    
    def load(self, df):
        pass
    
    def run(self):
        data = self.extract()
        transformed = self.transform(data)
        self.load(transformed)

class SalesPipeline(BasePipeline):
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder.appName("SalesPipeline").getOrCreate()

    def extract(self):
        try:
            return self.spark.read.csv(
                self.config["input_path"], 
                header=True, 
                inferSchema=True
            )
        except Exception as e:
            raise
    
    def transform(self, df):
        """Compute total unit sold for a product"""
        try:
            return df.groupBy("product_id").agg(sum("quantity").alias('total_unit_sold'))
        except Exception as e:
            raise

    def load(self, df):
        """Load data into  parquet file"""
        df.write.mode("overwrite") \
        .parquet(
            self.config["output_path"], 
            compression="snappy"
            )

if __name__ == "__main__":
   
    config = {
    "input_path": "/Users/thomasrivieres/clean-data-pipelines/.data/sales.csv",
    "output_path": f"/Users/thomasrivieres/clean-data-pipelines/.data/{datetime.date.today()}"
    }

    pipeline = SalesPipeline(config)
    pipeline.run()
