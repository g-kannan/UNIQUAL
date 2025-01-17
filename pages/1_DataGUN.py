from utils.common_imports import *
from utils.file_utils import *

st.title("DataGUN :material/analytics:")

file_format_list = ["parquet", "csv", "json", "delta"]


file_format = st.selectbox("Select file format", file_format_list)
s3_path_input = st.text_input("Enter S3 path", value=S3_PATHS[0])
s3_path = st.selectbox("Or select from dropdown", S3_PATHS, key=s3_path_input) if s3_path_input in S3_PATHS else s3_path_input

if st.button("Preview Data"):
    test_path="s3://tgt-southdms/input/hugging_face_data/gitrenum/*.csv"
    df = read_s3_path(s3_path, file_format)
    st.dataframe(df)