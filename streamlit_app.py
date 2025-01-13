import streamlit as st
from file_utils import read_s3_path
from dotenv import load_dotenv
import polars as pl
import os

load_dotenv() 
st.set_page_config(page_title="UNIQUAL", layout="wide")


st.title("UNIQUAL")
st.subheader("One Data quality tool for data enginners")

file_format_list = ["parquet", "csv", "json", "delta"]
S3_PATHS = os.getenv("S3_PATHS").split(',')

file_format = st.selectbox("Select file format", file_format_list)
s3_path = st.selectbox("Select S3 path", S3_PATHS)

if st.button("Preview Data"):
    df = read_s3_path(s3_path, file_format)
    st.dataframe(df)
