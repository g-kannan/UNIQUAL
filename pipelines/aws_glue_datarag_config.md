## Job parameters

| Key | Value | Description |
| --- | --- | --- |
| `--additional-python-modules` | `cuallee==0.7.1` | Required Python package for data quality checks |
| `--datalake-formats` | `delta` | Enables Delta Lake format support |
| `--datarag_path` | `<your-bucket-name>/path_to_datarag/` | S3 path for DataRAG storage (replace placeholder) |


## Spark Session
```markdown
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .getOrCreate()
```

