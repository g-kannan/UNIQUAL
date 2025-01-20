import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


def initialize_health_client(aws_access_key, aws_secret_key, region):
    """
    Initialize the AWS Health client and verify the connection.

    Args:
        aws_access_key (str): AWS access key ID.
        aws_secret_key (str): AWS secret access key.
        region (str): AWS region name.

    Returns:
        botocore.client.Health: The initialized AWS Health client.

    Raises:
        ValueError: If the connection to the AWS Health service fails.
    """
    try:
        # Create a Boto3 session using the provided credentials and region
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

        # Initialize the AWS Health client
        health_client = session.client('health')

        # # Make a test API call to verify the connection
        health_client.describe_events(maxResults=10)
        print("Successfully connected to AWS Health service.")

        return health_client

    except NoCredentialsError:
        raise ValueError("AWS credentials are not provided or invalid.")
    except PartialCredentialsError:
        raise ValueError("Incomplete AWS credentials provided.")
    except ClientError as e:
        raise ValueError(f"Failed to connect to AWS Health service: {str(e)}")
