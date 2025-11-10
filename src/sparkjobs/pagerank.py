from dataclasses import dataclass
from typing import Optional, List, Tuple
import time
import logging
from pyspark.sql import SparkSession
from pyspark import StorageLevel, RDD

from .io import EdgeReader
from .exceptions import InvalidInputError

logger = logging.getLogger(__name__)

@dataclass
class PageRankOptions:
    edges_path: str
    iterations: int = 10
    damping: float = 0.85
    top_k: int = 10
    persist: Optional[str] = None  # e.g. 'MEMORY_ONLY', 'MEMORY_AND_DISK'
    shuffle_partitions: Optional[int] = None

class PageRankJob:
    def __init__(self, spark: SparkSession, opts: PageRankOptions) -> None:
        if not opts.edges_path:
            raise InvalidInputError("edges_path is required")
        self.spark = spark
        self.opts = opts

        if opts.shuffle_partitions is not None:
            self.spark.conf.set("spark.sql.shuffle.partitions", str(opts.shuffle_partitions))

    def _storage_level(self) -> Optional[StorageLevel]:
        if not self.opts.persist:
            return None
        name = self.opts.persist.upper()
        if not hasattr(StorageLevel, name):
            raise InvalidInputError(f"Invalid persistence level: {self.opts.persist}")
        return getattr(StorageLevel, name)

    def run(self) -> List[Tuple[int, float]]:
        start = time.time()

        edges: RDD = EdgeReader(self.spark).read(self.opts.edges_path)
        links = edges.distinct().groupByKey().cache()

        # init ranks
        ranks = links.mapValues(lambda _: 1.0)

        # optional persistence
        level = self._storage_level()
        if level is not None:
            links.persist(level)
            ranks.persist(level)

        for _ in range(self.opts.iterations):
            contribs = links.join(ranks).flatMap(
                lambda node: (
                    (nbr, node[1][1] / len(node[1][0]))
                    for nbr in node[1][0]
                )
            )
            ranks = contribs.reduceByKey(lambda a, b: a + b).mapValues(
                lambda r: (1 - self.opts.damping) + self.opts.damping * r
            )
            if level is not None:
                ranks.persist(level)

        # collect and sort
        results = ranks.collect()
        results.sort(key=lambda x: x[1], reverse=True)
        top = results[: self.opts.top_k]

        # cleanup
        links.unpersist(False)
        if level is not None:
            ranks.unpersist(False)

        elapsed = time.time() - start
        logger.info("PageRank finished in %.2fs with %d iterations", elapsed, self.opts.iterations)
        return top
