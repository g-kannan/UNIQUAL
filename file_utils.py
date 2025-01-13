import os
from dotenv import load_dotenv
import polars as pl
import duckdb

load_dotenv() 

def read_s3_path(path,file_format):
    try:
        storage_options = {
            "AWS_ACCESS_KEY_ID":  os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "AWS_REGION": os.getenv("AWS_REGION"),
        }
        if file_format == "parquet":
            df = pl.read_parquet(path,storage_options=storage_options)
        elif file_format == "csv":
            df = pl.read_csv(path,storage_options=storage_options)
        elif file_format == "json":
            df = pl.read_json(path,storage_options=storage_options)
        elif file_format == "delta":
            df = pl.read_delta(path,storage_options=storage_options)
        else:
            raise Exception("Invalid file format")
        return df
    except Exception as e:
        raise Exception(f"Error occurred while reading path: {e}")