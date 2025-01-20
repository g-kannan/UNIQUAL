from utils.common_imports import *
from utils.file_utils import *
from datetime import datetime


st.title("DataRAG :material/tactic:")

s3_path = st.selectbox("Select Object storage path", S3_PATHS)

datarag_assets_path = s3_path+'datarag_assets/'
datarag_results_path = s3_path+'datarag_results/'

datarag_paths = [datarag_assets_path, datarag_results_path]

datarag_paths_selection = st.selectbox("Select DataRAG path", datarag_paths)
file_format='delta'

if st.button("Preview Data"):
    df = read_s3_path(datarag_paths_selection, file_format)
    # Store the DataFrame in session state
    st.session_state["df"] = df

# Check if the DataFrame is available in session state
if "df" in st.session_state:
    df = st.session_state["df"]

    # Ensure the STATUS column exists
    if 'STATUS' in df.columns:
        editable_column = "STATUS"  # Specify the column you want to make editable
        non_editable_columns = [col for col in df.columns if col != "STATUS"]
        allowed_values = ["R", "A", "G"]  # Dropdown values

        # Streamlit editable table with dropdown for the STATUS column
        edited_df = st.data_editor(
            df.to_pandas(),  # Streamlit does not natively support Polars yet
            column_config={
                editable_column: st.column_config.SelectboxColumn(
                    "STATUS",
                    options=allowed_values,
                )
            },
            disabled=non_editable_columns,  # Disable editing for other columns
            key="data_editor",
        )

        # Button to save changes
        if st.button("Save Changes"):
            # Convert edited DataFrame back to Polars
            updated_df = pl.DataFrame(edited_df)

            # Identify rows where STATUS has changed
            original_df = st.session_state["df"]
            updated_df = updated_df.with_columns([
                pl.when(pl.col("STATUS") != original_df["STATUS"])
                .then(pl.lit("UI"))
                .otherwise(pl.col("UPDATED_BY"))
                .alias("UPDATED_BY"),

                pl.when(pl.col("STATUS") != original_df["STATUS"])
                .then(pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                .otherwise(pl.col("UPDATED_TS"))
                .alias("UPDATED_TS")
            ])
            
            # Save the modified DataFrame back to the data lake
            save_s3_path(datarag_paths_selection, updated_df, file_format)
            st.success("Changes saved to Delta Lake!")
    else:
        st.dataframe(df)
else:
    st.info("Click 'Preview Data' to load and display the data.")
