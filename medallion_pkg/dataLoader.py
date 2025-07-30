#Crear la clase para Spark
from pyspark.sql import SparkSession

class SparkDataLoader:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.spark = SparkSession.builder \
            .appName("SparkParquetLoader") \
            .getOrCreate()

    def load_csv_as_rdd(self):
        rdd = self.spark.sparkContext.textFile(self.input_path)
        header = rdd.first()
        data = rdd.filter(lambda row: row != header)
        print(f"RDD count: {data.count()}")
        return data

    def load_and_save_parquet(self):
        df = self.spark.read.option("header", True).csv(self.input_path)
        df.write.mode("overwrite").parquet(self.output_path)
        print(f"Spark guardó el DataFrame como Parquet en: {self.output_path}")

#Crear la clase para Pandas
import pandas as pd

class PandasDataLoader:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path

    def load_and_save_parquet(self):
        df = pd.read_csv(self.input_path)
        df.to_parquet(self.output_path, engine="pyarrow", index=False)
        print(f"Pandas guardó el DataFrame como Parquet en: {self.output_path}")