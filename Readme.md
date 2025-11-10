# spark-jobs-oop

Object-oriented, production-grade refactor of:
- **PageRank** jobs (partition tuning, persistence, failure simulation hooks)
- **IoT CSV sort** job (sort by `cca2` then `timestamp` and write back to HDFS)


## CLI
```bash
# PageRank (RDD-based)
spark-submit --master spark://master:7077 -c spark.executor.instances=3 \
  -c spark.executor.cores=4 -c spark.executor.memory=8g \
  -c spark.dynamicAllocation.enabled=false \
  -m 4g -d 4g \
  -py-files dist/spark-jobs-oop-0.1.0-py3-none-any.whl \
  -v ./src/sparkjobs/cli.py pagerank \
  --edges hdfs://nn:9000/user/hadoop/dataset/web-BerkStan.txt \
  --iterations 10 --damping 0.85 --top-k 10 --shuffle-partitions 50 --persist MEMORY_ONLY

# IoT sorter
spark-submit --master spark://master:7077 \
  -py-files dist/spark-jobs-oop-0.1.0-py3-none-any.whl \
  -v ./src/sparkjobs/cli.py iot-sort \
  --input hdfs://nn:9000/export.csv --output hdfs://nn:9000/sorted_output.csv
```

## Code layout
```
src/sparkjobs/
  cli.py                 # argparse-based entrypoint
  spark.py               # SparkSessionFactory
  pagerank.py            # PageRankJob (RDD-based)
  iot_sort.py            # IoTDataSorter
  io.py                  # Readers/Writers
  config.py              # Config + dataclasses for options
  logging_utils.py       # Structured logging formatter
  exceptions.py          # Custom exceptions
```
