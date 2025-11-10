from typing import Iterable, Tuple
from pyspark.sql import SparkSession
from pyspark import RDD

class EdgeReader:
    """Reads (fromId, toId) edges from a text file with lines 'u\t v'. Skips comment lines starting with '#'."""
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(self, path: str) -> RDD:
        rdd = self.spark.read.text(path).rdd
        edges = (
            rdd.filter(lambda x: not x.value.startswith("#"))
               .map(lambda x: x.value.split("\t"))
               .map(lambda x: (int(x[0]), int(x[1])))
        )
        return edges

class CSVReader:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(self, path: str, header: bool = True):
        return self.spark.read.option("header", str(header).lower()).csv(path)

class CSVWriter:
    def __init__(self, header: bool = True) -> None:
        self.header = header

    def write(self, df, path: str) -> None:
        df.write.option("header", str(self.header).lower()).csv(path)
