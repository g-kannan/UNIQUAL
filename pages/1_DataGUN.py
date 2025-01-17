from utils.common_imports import *
from utils.file_utils import *

st.title("DataGUN")

file_format_list = ["parquet", "csv", "json", "delta"]


file_format = st.selectbox("Select file format", file_format_list)
s3_path = st.selectbox("Select S3 path", S3_PATHS)

if st.button("Preview Data"):
    df = read_s3_path(s3_path, file_format)
    st.dataframe(df)