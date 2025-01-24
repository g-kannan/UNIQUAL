from utils.common_imports import *
from utils.aws_health_utils import *
import pandas as pd
import io

# TODO: i) Saving File(s) to parquet/ORC [Done].
#       ii) Sending SES Alerts to interested parties.
#       iii) Adding option to send the report to S3.
#       iv) Add functionality of seeing all used services in the account.

st.title("AWS Health :material/health_and_safety:")

# Add a button to trigger event fetching
if st.button("Get All Events"):
    # Fetch events
    events = get_all_events()
    st.dataframe(events)

    # Display number of events
    st.write(f"Number of Events: {len(events)}")

events = get_all_events()
df = pd.DataFrame(events)

# User Option Selection
option = st.radio(
    "Choose an option:",
    ["Download Full Data", "Filter Data and Download"]
)

# Format Selection
download_format = st.radio("Select Download Format:", ["CSV", "Parquet"], horizontal=True)

# Function to generate download data
def generate_download_data(dataframe, file_format):
    """
    Convert a pandas DataFrame to a downloadable file in the specified format.
    
    Parameters:
        dataframe (pandas.DataFrame): The DataFrame to be converted and downloaded
        file_format (str): The desired output file format, either "CSV" or "Parquet"
    
    Returns:
        tuple: A tuple containing:
            - Encoded file content (bytes)
            - MIME type of the file (str)
    
    Raises:
        ValueError: If an unsupported file format is specified
    
    Notes:
        - For CSV, the file is encoded in UTF-8 with no index column
        - For Parquet, the file is written to a BytesIO buffer with no index column
        - Supports CSV and Parquet formats
        - ORC format is currently commented out
    """
    if file_format == "CSV":
        return dataframe.to_csv(index=False).encode("utf-8"), "text/csv"
    elif file_format == "Parquet":
        buffer = io.BytesIO()
        dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)
        return buffer.read(), "application/octet-stream"
    # elif file_format == "ORC":
    #     buffer = io.BytesIO()
    #     dataframe.to_orc(buffer, index=False)
    #     buffer.seek(0)
    #     return buffer.read(), "application/octet-stream"

if option == "Download Full Data":
    st.write("Click below to download the full dataset:")
    data, mime_type = generate_download_data(df, download_format)
    st.download_button(
        label=f"Download Full Data as {download_format}",
        data=data,
        file_name=f"aws_events_full.{download_format.lower()}",
        mime=mime_type,
    )
elif option == "Filter Data and Download":
    # Filter by event type
    event_types = list(df["eventTypeCategory"].unique())
    selected_type = st.selectbox("Filter by Event Type:", ["All"] + event_types)

    # Filter logic
    filtered_df = df if selected_type == "All" else df[df["eventTypeCategory"] == selected_type]

    # Display filtered data
    st.write(f"Number of Events: {len(filtered_df)}")
    st.dataframe(filtered_df)

    # Download button for filtered data
    data, mime_type = generate_download_data(filtered_df, download_format)
    st.download_button(
        label=f"Download Filtered Data as {download_format}",
        data=data,
        file_name=f"aws_events_filtered.{download_format.lower()}",
        mime=mime_type,
    )