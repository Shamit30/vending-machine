from dataclasses import dataclass
from typing import Optional

@dataclass
class SparkConfig:
    app_name: str = "SparkJobs"
    master: Optional[str] = None
    executor_instances: Optional[int] = None
    executor_cores: Optional[int] = None
    executor_memory: Optional[str] = None
    driver_memory: Optional[str] = None
    shuffle_partitions: Optional[int] = None
    dynamic_allocation: Optional[bool] = None

    def as_dict(self) -> dict:
        d = {}
        if self.master: d["spark.master"] = self.master
        if self.executor_instances is not None:
            d["spark.executor.instances"] = str(self.executor_instances)
        if self.executor_cores is not None:
            d["spark.executor.cores"] = str(self.executor_cores)
        if self.executor_memory:
            d["spark.executor.memory"] = self.executor_memory
        if self.driver_memory:
            d["spark.driver.memory"] = self.driver_memory
        if self.shuffle_partitions is not None:
            d["spark.sql.shuffle.partitions"] = str(self.shuffle_partitions)
        if self.dynamic_allocation is not None:
            d["spark.dynamicAllocation.enabled"] = str(self.dynamic_allocation).lower()
        return d
