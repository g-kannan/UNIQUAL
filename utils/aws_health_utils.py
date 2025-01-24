import os
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from utils.boto3_sessions import initialize_health_client, initialize_ses_client, initialize_s3_client
from dotenv import load_dotenv
load_dotenv()

# AWS credentials
aws_access_key=os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY")
region=os.getenv("AWS_REGION")

try:
    health_client = initialize_health_client(aws_access_key, aws_secret_key, region)
except ValueError as e:
    print(f"Error: {e}")

# Get current time and 1 year ago for filtering
current_time = datetime.utcnow()
start_time = current_time - timedelta(days=360)


# Fetch all events with pagination
def get_all_events():
    all_events = []
    paginator = health_client.get_paginator('describe_events')

    # Using OR conditions for all event types and statuses
    filter_criteria = {
        'filter': {
            'eventTypeCategories': [
                'issue',
                'scheduledChange',
                'accountNotification',
                'investigation'
            ],
            'eventStatusCodes': [
                'open',
                'closed',
                'upcoming'
            ],
            'startTimes': [{'from': start_time}]
        }
    }

    try:
        # Get all pages of results
        for page in paginator.paginate(**filter_criteria):
            # Process each event in the page
            for event in page['events']:
                # Get additional details for each event
                details = health_client.describe_event_details(
                    eventArns=[event['arn']]
                )

                # Add description if available
                if details['successfulSet']:
                    event['description'] = details['successfulSet'][0]['eventDescription']['latestDescription']

                # Get affected entities
                entities = health_client.describe_affected_entities(
                    filter={'eventArns': [event['arn']]}
                )
                event['affected_entities'] = len(entities['entities'])

                all_events.append(event)

        print(f"Found {len(all_events)} events")
        return all_events

    except Exception as e:
        print(f"Error getting events: {str(e)}")
        return []


try:
    ses_client = initialize_ses_client(aws_access_key, aws_secret_key, region)
except ValueError as e:
    print(f"Error: {e}")


def send_email_via_ses(sender_email, recipient_email, subject, body):
    """
    Send an email via AWS SES.

    Args:
        sender_email (str): The sender's email address (must be verified in SES).
        recipient_email (str): The recipient's email address.
        subject (str): Email subject.
        body (str): Email body.

    Raises:
        ValueError: If the email could not be sent.
    """
    try:
        # Send the email
        response = ses_client.send_email(
            Source=sender_email,
            Destination={'ToAddresses': [recipient_email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        print(f"Email sent! Message ID: {response['MessageId']}")
    except Exception as e:
        raise ValueError(f"Failed to send email: {e}") from e


def get_all_events_and_notify(df, sender_email, recipient_email):
    """
    Process all events in the provided DataFrame, and if the event type is 'scheduledChange',
    send an email notification.

    Args:
        df (pd.DataFrame): DataFrame containing AWS Health events.
        sender_email (str): The sender's email address (must be verified in SES).
        recipient_email (str): The recipient's email address.
    """
    try:
        # Filter the DataFrame for scheduledChange events
        scheduled_change_events = df[df['eventTypeCategory'] == 'scheduledChange']

        # Iterate through the filtered events
        for _, event in scheduled_change_events.iterrows():
            event_description = event.get('eventDescription', 'No description available.')
            subject = f"Scheduled Change Notification: {event['eventTypeCategory']}"
            body = f"Event ARN: {event['arn']}\nDescription: {event_description}"

            # Send email notification for each scheduledChange event
            send_email_via_ses(sender_email, recipient_email, subject, body)
            print(f"Notification sent for event ARN: {event['arn']}")

    except Exception as e:
        print(f"Error processing events or sending notifications: {str(e)}")

try:
    s3_client = initialize_s3_client(aws_access_key, aws_secret_key, region)
except ValueError as e:
    print(f"Error: {e}")

def save_df_to_s3(df, bucket_name, prefix):
    """
    Save a Pandas DataFrame to an S3 bucket in both CSV and Parquet formats.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        bucket_name (str): Name of the S3 bucket.
        prefix (str): Prefix (folder path) for storing the files in the bucket.

    Raises:
        ValueError: If the S3 bucket name or prefix is invalid.
    """
    try:
        # Validate inputs
        if not bucket_name or not prefix:
            raise ValueError("Bucket name and prefix cannot be empty.")

        # Save DataFrame as CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        csv_key = f"{prefix}/data.csv"
        s3_client.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())
        print(f"CSV file saved to s3://{bucket_name}/{csv_key}")

        # Save DataFrame as Parquet
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
        parquet_key = f"{prefix}/data.parquet"
        s3_client.put_object(Bucket=bucket_name, Key=parquet_key, Body=parquet_buffer.getvalue())
        print(f"Parquet file saved to s3://{bucket_name}/{parquet_key}")

    except Exception as e:
        print(f"Error saving DataFrame to S3: {str(e)}")