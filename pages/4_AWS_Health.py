from utils.common_imports import *
from utils.aws_health_utils import *
import pandas as pd

st.title("AWS Health :material/health_and_safety:")

events = get_all_events()
df = pd.DataFrame(events)

# Add a button to trigger event fetching
if st.button("Get All Events"):
    # Fetch events
    events = get_all_events()

    # Display number of events
    st.write(f"Number of Events: {len(events)}")


# User Option Selection
option = st.radio(
    "Choose an option:",
    ["Download Full Data", "Filter Data and Download"]
)

if option == "Download Full Data":
    st.write("Click below to download the full dataset:")
    st.download_button(
        label="Download Full Data as CSV",
        data=df.to_csv(index=False),
        file_name="aws_events_full.csv",
        mime="text/csv",
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
    st.download_button(
        label="Download Filtered Data as CSV",
        data=filtered_df.to_csv(index=False),
        file_name="aws_events_filtered.csv",
        mime="text/csv",
    )