from typing import Optional, Dict
from pyspark.sql import SparkSession

from .config import SparkConfig

class SparkSessionFactory:
    @staticmethod
    def build(config: Optional[SparkConfig] = None) -> SparkSession:
        config = config or SparkConfig()
        builder = SparkSession.builder.appName(config.app_name)
        for k, v in config.as_dict().items():
            # 'spark.master' requires .master() specifically
            if k == "spark.master" and v:
                builder = builder.master(v)
            else:
                builder = builder.config(k, v)
        return builder.getOrCreate()
