from urllib.parse import urlparse
import boto3

__all__ = [
    "move_object",
    "list_1000_objects",
    "download_object",
    "is_s3",
    "get_s3_bucket",
    "get_s3_key",
    "delete_object"
]

s3 = boto3.resource('s3')


def move_object(key, source_bucket, dest_bucket):
    # Move objects between buckets
    copy_source = {
        'Bucket': source_bucket,
        'Key': key
    }
    s3.meta.client.copy(copy_source, dest_bucket, key)
    delete_object(source_bucket, key)

    return True


def list_1000_objects(bucket, prefix):
    response = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get('Contents')
    return objects


def download_object(bucket, key, destination):
    s3.Object(bucket, key).download_file(destination)


def delete_object(bucket, key):
    s3.Object(bucket, key).delete()


def is_s3(url):
    parsed_url = urlparse(url)
    return parsed_url.scheme == 's3'


def get_s3_bucket(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc


def get_s3_key(url):
    parsed_url = urlparse(url)
    return parsed_url.path.strip('/')
