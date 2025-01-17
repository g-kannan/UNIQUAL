from utils.common_imports import *

def create_duckdb_connection_with_s3():
    KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
    SECRET=os.getenv("AWS_SECRET_ACCESS_KEY")
    REGION=os.getenv("AWS_REGION")
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{KEY_ID}',
            SECRET '{SECRET}',
            REGION '{REGION}'
        );
        """)
    return conn