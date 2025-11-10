from dataclasses import dataclass
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

from .io import CSVReader, CSVWriter
from .exceptions import InvalidInputError

logger = logging.getLogger(__name__)

@dataclass
class IoTOptions:
    input_path: str
    output_path: str
    timestamp_col: str = "timestamp"
    country_col: str = "cca2"

class IoTDataSorter:
    def __init__(self, spark: SparkSession, opts: IoTOptions) -> None:
        if not opts.input_path or not opts.output_path:
            raise InvalidInputError("Both input_path and output_path are required")
        self.spark = spark
        self.opts = opts

    def run(self) -> None:
        reader = CSVReader(self.spark)
        writer = CSVWriter(header=True)

        df = reader.read(self.opts.input_path)
        df = df.withColumn(self.opts.timestamp_col, col(self.opts.timestamp_col).cast("long"))
        sorted_df = df.orderBy(col(self.opts.country_col).asc(), col(self.opts.timestamp_col).asc())

        logger.info("Writing sorted result to %s", self.opts.output_path)
        writer.write(sorted_df, self.opts.output_path)
