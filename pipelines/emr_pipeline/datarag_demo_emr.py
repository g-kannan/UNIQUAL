import sys
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import lit, col, expr
from delta.tables import DeltaTable
from cuallee import Check, CheckLevel
from pyspark.sql.utils import AnalysisException
import argparse
import json

# Getting the DataRAG path in Run time
parser = argparse.ArgumentParser(
        prog="DataRag",
        description="For passing DataRAG base path",
    )

parser.add_argument("-datarag_path", "--datarag_path",
                    help="S3 Path of DataRAG, ex:Bucket_name/prefix/",
                    type=str,
                    default="rzp-emr-conf/rag/test1")


def get_cluster_id():
    # Path to the job-flow.json file
    job_flow_file_path = "/mnt/var/lib/info/job-flow.json"

    # Open the job-flow.json file and load its content
    with open(job_flow_file_path, 'r') as file:
        job_flow_data = json.load(file)

    # Extract the jobFlowId from the JSON data
    cluster_id = job_flow_data.get("jobFlowId")

    if cluster_id:
        return cluster_id
    else:
        print("Cluster ID not found in the job-flow.json file.")
        return None


# parsing thr arguments passed
args = parser.parse_args()

# Creating a spark Session
spark = SparkSession.builder.getOrCreate()

# Fetching the arguments
job_flow_id = get_cluster_id()
datarag_path = args.datarag_path
spark_app_id = spark.sparkContext.applicationId
spark_app_name = spark.sparkContext.appName

# Read the input data
df = spark.read.format("csv").load("s3://sheik-harris-raw-zone/csv/repo_stats_*.csv",
                                   inferSchema=True,
                                   header=True)

# Creating the checks
check = Check(CheckLevel.ERROR)
check.is_complete("repo_name")
check.is_greater_than("release_event_count", 0, pct=0.5)

# Validate the check in input data
result_df = check.validate(df)


final_df = result_df \
    .withColumn("etl_tool", lit("EMR")) \
    .withColumn("object_type", lit("pipeline")) \
    .withColumn("object_name", lit(spark_app_name)) \
    .withColumn("runid", lit(f"{job_flow_id}/{spark_app_id}"))

final_df.show()


final_df.createOrReplaceTempView("dataquality_audit_vw")

final_df.write.format("delta")\
    .mode("append")\
    .option("path", f"s3://{datarag_path}/datarag_results/")\
    .save()

print("INFO : Saved DataRAG Results")


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
""")

rag_df.show()

# datarag_asset
datarag_asset_path = f"s3://{datarag_path}/datarag_assets/"

# create table if not exists for first time merge issue
try:
    delta_table = DeltaTable.forPath(spark, datarag_asset_path)
    print(f"INFO: Delta table found at {datarag_asset_path}")
except AnalysisException:
    print(f"Delta table not found at {datarag_asset_path}. Creating a new one")
    rag_df.write.format("delta").mode("overwrite").option("path", f"{datarag_asset_path}").save()
    delta_table = DeltaTable.forPath(spark, datarag_asset_path)  # Do we need this line
    delta_table.delete("1=1")  # Do we need this line

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

