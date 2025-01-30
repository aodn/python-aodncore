#!/usr/bin/env python

import argparse
import logging

from aodncore.util.aws import upload_to_s3

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload a file to an AWS S3 bucket.')
    parser.add_argument('local_file', type=str, help='Path to the local file')
    parser.add_argument('-b', '--bucket', type=str, help='Name of the S3 bucket',
                        default="aodn-dataflow-dev")
    parser.add_argument('-k', '--s3-key', type=str,
                        help='Key in the S3 bucket (default to local path')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')

    args = parser.parse_args()

    if not args.s3_key:
        args.s3_key = args.local_file

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger('boto3').setLevel(logging.DEBUG)
        logging.getLogger('botocore').setLevel(logging.DEBUG)

    upload_to_s3(args.local_file, args.bucket, key=args.s3_key)
