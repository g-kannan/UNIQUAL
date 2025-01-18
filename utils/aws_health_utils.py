import os
from datetime import datetime, timedelta
from utils.boto3_sessions import initialize_health_client
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

