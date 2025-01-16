import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit,col,expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# For Delta Lake
from delta.tables import DeltaTable

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','datarag_path'])
job_run_id = args['JOB_RUN_ID']
print(job_run_id)


# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# # Set Delta configurations programmatically
# spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
# spark.conf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
# spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")

spark = SparkSession \
.builder \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
.getOrCreate()

# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

# print(job)

print(f"Current Job Name: {args['JOB_NAME']}")

from cuallee import Check,CheckLevel
df = spark.read.format("csv").options(inferSchema='true', header='true').option("path","s3://tgt-southdms/input/hugging_face_data/gitrenum/*.csv").load()
check = Check(CheckLevel.ERROR)
check.is_complete("repo_name")
check.is_greater_than("release_event_count",0,pct=0.5)

# Validate
result_df = check.validate(df)
final_df = result_df.withColumn("etl_tool",lit("Glue")).withColumn("object_type",lit("pipeline")).withColumn("object_name",lit(args['JOB_NAME'])).withColumn("runid",lit(job_run_id))
final_df.show()
final_df.createOrReplaceTempView("dataquality_audit_vw")

final_df.write.format("delta").mode("append").option("path",f"s3://{args['datarag_path']}/datarag_results/").save()

# datarag_assets_create_query = (f"""
#   CREATE OR REPLACE TABLE  datarag_assets (ASSET_TYPE STRING, ASSET_NAME STRING,PASSCOUNT INT,FAILCOUNT INT, STATUS CHAR(1), UPDATED_BY STRING, CREATED_TS TIMESTAMP, UPDATED_TS TIMESTAMP) 
#   USING delta
#   LOCATION '{datarag_asset_path}'
# """)

# print("Create datarag_assets table if not exists using query: "+ datarag_assets_create_query)
# spark.sql(datarag_assets_create_query).show()

rag_df = spark.sql("""
WITH AssetCounts AS (
    SELECT 
        object_type AS ASSET_TYPE,
        object_name AS ASSET_NAME,
        COUNT(CASE WHEN status = 'PASS' THEN 1 END) AS PASSCOUNT,
        COUNT(CASE WHEN status = 'FAIL' THEN 1 END) AS FAILCOUNT
    FROM 
        dataquality_audit_vw
    GROUP BY 
        object_type, object_name
)
SELECT 
    ASSET_TYPE,
    ASSET_NAME,
    PASSCOUNT,
    FAILCOUNT,
    CASE 
    WHEN FAILCOUNT > 0 THEN 'R' 
    WHEN FAILCOUNT = 0 THEN 'G'
    END AS STATUS,
    current_user() AS UPDATED_BY,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS CREATED_TS,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS UPDATED_TS
FROM 
    AssetCounts;
--group by 1,2
""")

rag_df.show()

#datarag_asset
datarag_asset_path = f"s3://{args['datarag_path']}/datarag_assets/"

# test_df = spark.read.format("delta").option("path",datarag_asset_path).load()
# test_df.show()

# create table if not exists for first time merge issue
try:
    delta_table = DeltaTable.forPath(spark, datarag_asset_path)
except:
    # Define the schema
    # schema = StructType([
    #     StructField("ASSET_TYPE", StringType(), True),
    #     StructField("ASSET_NAME", StringType(), True),
    #     StructField("PASSCOUNT", IntegerType(), True),
    #     StructField("FAILCOUNT", IntegerType(), True),
    #     StructField("STATUS", StringType(), True),
    #     StructField("UPDATED_BY", StringType(), True),
    #     StructField("CREATED_TS", TimestampType(), True),
    #     StructField("UPDATED_TS", TimestampType(), True)
    # ])
    rag_df.write.format("delta").mode("overwrite").option("path",f"{datarag_asset_path}").save()
    delta_table = DeltaTable.forPath(spark, datarag_asset_path)
    delta_table.delete("1=1")

# UPSERT process
delta_table = DeltaTable.forPath(spark, datarag_asset_path)
delta_table.alias("tgt").merge(
    source=rag_df.alias("src"),
    condition=expr("tgt.ASSET_TYPE = src.ASSET_TYPE and tgt.ASSET_NAME = src.ASSET_NAME")
    ).whenMatchedUpdate(
        set={
        "PASSCOUNT": col("src.PASSCOUNT"),
        "FAILCOUNT": col("src.FAILCOUNT"),
        "STATUS": col("src.STATUS"),
        "UPDATED_BY": col("src.UPDATED_BY"),
        "UPDATED_TS": col("src.UPDATED_TS")
            }
    ).whenNotMatchedInsertAll().execute()

# job.commit()