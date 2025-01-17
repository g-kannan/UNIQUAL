## Job parameters

| Key | Value |
| --- | --- |
| `--additional-python-modules` | `cuallee` |
| `--datalake-formats` | `delta`|
| `--datarag_path` | `bucketname/path_to_datarag/` |


## Spark Session
```markdown
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .getOrCreate()
```

