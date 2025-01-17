from utils.common_imports import *
from utils.file_utils import *

st.title("DataGUN :material/analytics:")

file_format_list = ["parquet", "csv", "json", "delta"]


file_format = st.selectbox("Select file format", file_format_list)
s3_path_input = st.text_input("Enter S3 path", value=S3_PATHS[0])
s3_path = st.selectbox("Or select from dropdown", S3_PATHS, key=s3_path_input) if s3_path_input in S3_PATHS else s3_path_input

if st.button("Preview Data"):
    if file_format in ["parquet","csv"]:
        df = read_s3_duckdb(s3_path,file_format)
        cols = df.columns.to_list()
        st.dataframe(df,hide_index=True)
        st.write("Columns in the dataset")
        st.json(cols)
    if file_format == "delta":
        df = read_s3_path(s3_path, file_format)
        st.dataframe(df,hide_index=True)