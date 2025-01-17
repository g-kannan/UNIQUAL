from utils.common_imports import *
from utils.file_utils import *

st.title("DataRAG")

s3_path = st.selectbox("Select S3 path", S3_PATHS)

datarag_assets_path = s3_path+'datarag_assets/'
datarag_results_path = s3_path+'datarag_results/'

datarag_paths = [datarag_assets_path, datarag_results_path]

datarag_paths_selection = st.selectbox("Select datarag path", datarag_paths)

if st.button("Preview Datarag Data"):
    df = read_s3_path(datarag_paths_selection, file_format='delta')
    st.dataframe(df)