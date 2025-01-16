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

datarag_assets_path = s3_path+'datarag_assets/'
datarag_results_path = s3_path+'datarag_results/'

datarag_paths = [datarag_assets_path, datarag_results_path]

if st.button("Preview Data"):
    df = read_s3_path(s3_path, file_format)
    st.dataframe(df)

datarag_paths_selection = st.selectbox("Select datarag path", datarag_paths)

if st.button("Preview Datarag Data"):
    df = read_s3_path(datarag_paths_selection, file_format='delta')
    st.dataframe(df)