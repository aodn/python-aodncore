import os
from typing import Optional

import boto3
from botocore.exceptions import NoCredentialsError


def upload_to_s3(local_file: str,
                 bucket_name: str,
                 bucket_prefix: Optional[str] = None,
                 key: Optional[str] = None,
                 aws_profile: Optional[str] = None) -> None:
    """
    Upload a file to an S3 bucket

    :param local_file: Full path to local file
    :param bucket_name: Bucket to upload to
    :param bucket_prefix: Prefix to add to key
    :param key: Path in bucket to upload to (defaults to local file name)
    :param aws_profile: AWS profile to use
    :return: None
    """

    if not key:
        key = os.path.basename(local_file)

    if bucket_prefix:
        key = f"{bucket_prefix}/{key}"

    session = boto3.session.Session(profile_name=aws_profile)
    s3 = session.client('s3')
    try:
        s3.upload_file(local_file, bucket_name, key)
        print(f"Upload Successful: {local_file} to {bucket_name}/{key}")
    except FileNotFoundError:
        print(f"The file {local_file} was not found")
    except NoCredentialsError:
        print("Credentials not available")
