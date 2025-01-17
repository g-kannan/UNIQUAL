from utils.common_imports import *
st.set_page_config(layout="wide")

st.title("UNIQUAL")
st.subheader("Copilot for data enginners")


st.write("Choose a page to get started")

st.page_link("pages/1_DataGUN.py", label="DataGUN",icon=":material/analytics:")
st.page_link("pages/2_DataRAG.py", label="DataRAG",icon=":material/tactic:")
st.page_link("pages/3_Snowflake.py", label="Snowflake",icon=":material/ac_unit:")

