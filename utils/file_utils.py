from utils.common_imports import *
from dotenv import load_dotenv
load_dotenv()
from utils.duckdb_utils import *
S3_PATHS = os.getenv("S3_PATHS").split(',')
print(S3_PATHS)

storage_options = {
            "AWS_ACCESS_KEY_ID":  os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "AWS_REGION": os.getenv("AWS_REGION"),
        }

def read_s3_path(path,file_format):
    try:
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

def read_s3_duckdb(path,file_format):
    try:
        conn = create_duckdb_connection_with_s3()
        if path.endswith('/'):
            path = path[:-1]
        if file_format=="csv":
            query = f"select * from read_csv('{path}/*.csv') limit 100"
        elif file_format=="parquet":
            query = f"select * from read_parquet('{path}/*.parquet') limit 100"
        st.write("Executing query: "+query)
        df = conn.sql(query).to_df()
        return df
    except Exception as e:
        raise Exception(f"Error occurred while reading path: {e}")

def save_s3_path(path,df,file_format):
    try:
        if file_format == "parquet":
            df.write_parquet(path,storage_options=storage_options,mode="overwrite")
        elif file_format == "csv":
            df.write_csv(path,storage_options=storage_options,mode="overwrite")
        elif file_format == "json":
            df.write_json(path,storage_options=storage_options,mode="overwrite")
        elif file_format == "delta":
            df.write_delta(path,storage_options=storage_options,mode="overwrite")
        else:
            raise Exception("Invalid file format")
        return df
    except Exception as e:
        raise Exception(f"Error occurred while writing to path: {e}")

