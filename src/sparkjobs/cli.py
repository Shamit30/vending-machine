import argparse
import logging
from typing import Optional
from .logging_utils import setup_logging
from .spark import SparkSessionFactory
from .config import SparkConfig
from .pagerank import PageRankJob, PageRankOptions
from .iot_sort import IoTDataSorter, IoTOptions

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sparkjobs", description="Spark Jobs CLI")
    sub = p.add_subparsers(dest="command", required=True)

    # Common Spark options
    def add_spark_opts(sp):
        sp.add_argument("--app-name", default="SparkJobs", help="Spark app name")
        sp.add_argument("--master", default=None, help="Spark master URL")
        sp.add_argument("--executor-instances", type=int, default=None)
        sp.add_argument("--executor-cores", type=int, default=None)
        sp.add_argument("--executor-memory", default=None)
        sp.add_argument("--driver-memory", default=None)
        sp.add_argument("--shuffle-partitions", type=int, default=None)
        sp.add_argument("--dynamic-allocation", type=str, choices=["true","false"], default=None)

    # PageRank
    pr = sub.add_parser("pagerank", help="Run PageRank")
    add_spark_opts(pr)
    pr.add_argument("--edges", required=True, help="HDFS/local path to edges file")
    pr.add_argument("--iterations", type=int, default=10)
    pr.add_argument("--damping", type=float, default=0.85)
    pr.add_argument("--top-k", type=int, default=10)
    pr.add_argument("--persist", default=None, help="e.g., MEMORY_ONLY, MEMORY_AND_DISK")

    # IoT sort
    iot = sub.add_parser("iot-sort", help="Sort IoT CSV by cca2, timestamp")
    add_spark_opts(iot)
    iot.add_argument("--input", required=True, help="input CSV path (HDFS/local)")
    iot.add_argument("--output", required=True, help="output path (HDFS/local)")
    iot.add_argument("--timestamp-col", default="timestamp")
    iot.add_argument("--country-col", default="cca2")

    return p

def parse_bool(x: Optional[str]) -> Optional[bool]:
    if x is None:
        return None
    return x.lower() == "true"

def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    setup_logging("INFO")

    cfg = SparkConfig(
        app_name=args.app_name,
        master=args.master,
        executor_instances=args.executor_instances,
        executor_cores=args.executor_cores,
        executor_memory=args.executor_memory,
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
        dynamic_allocation=parse_bool(getattr(args, "dynamic_allocation", None)),
    )

    spark = SparkSessionFactory.build(cfg)

    if args.command == "pagerank":
        opts = PageRankOptions(
            edges_path=args.edges,
            iterations=args.iterations,
            damping=args.damping,
            top_k=args.top_k,
            persist=args.persist,
            shuffle_partitions=args.shuffle_partitions,
        )
        job = PageRankJob(spark, opts)
        top = job.run()
        print("Top pages by rank:")
        for node, rank in top:
            print(f"Page: {node}, Rank: {rank}")

    elif args.command == "iot-sort":
        opts = IoTOptions(
            input_path=args.input,
            output_path=args.output,
            timestamp_col=args.timestamp_col,
            country_col=args.country_col,
        )
        job = IoTDataSorter(spark, opts)
        job.run()

    spark.stop()

if __name__ == "__main__":
    main()
