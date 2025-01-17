import os
from dotenv import load_dotenv
import polars as pl
import duckdb
import streamlit as st
from utils.file_utils import read_s3_path
import fsspec
import s3fs

load_dotenv() 
