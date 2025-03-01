from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class DataSource(ABC):
    @abstractmethod
    def read(self) -> DataFrame:
        pass

class LocalFileSource(DataSource):
    def __init__(self, path:str):
        self.path = path
    
    def read(self) -> DataFrame:
        return spark.read.csv(self.path, header=True)

class SourceS3(DataSource):
    def __init__(self, bucket:str, key:str) ->None:
        self.path = f"s3a://{bucket}/{key}"

    def read(self) -> DataFrame:
        return spark.read.parquet(self.path)
    
class APIsource(DataSource):
    def __init__(self, url: str, api_key:str) -> None:
        self.url = url
        self.api_key = api_key

    def read(self):
        data = [("prod1", 10), ("prod2", 15)]
        return spark.createDataFrame(data, , ["product_id", "quantity"])

