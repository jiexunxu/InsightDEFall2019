# initializes s3 bucket, postgreSQL database and a lot of other internal values used by BOSA

import boto3
import psycopg2
import read_credentials
import time
import random


def init():
    def randomString(stringLength):
        letters = string.ascii_letters
        return "".join(random.choice(letters) for i in range(stringLength))

    # If there are more than internal_params[0] images to process, save them as individual files on s3, else save them locally and upload to s3 as a zip file
    internal_params = [200]
    [
        aws_key_id,
        aws_access_key,
        db_host,
        db_password,
        instanceIds,
        enable_ec2_control,
    ] = read_credentials.read()
    s3 = boto3.resource(
        "s3", aws_access_key_id=aws_key_id, aws_secret_access_key=aws_access_key
    )
    bucket = s3.Bucket("jiexunxu-open-image-dataset")
    connection = psycopg2.connect(
        host=db_host, database="imagedb", user="postgres", password=db_password
    )
    # Use current time plus a 10-character random string to save each processed request in a unique folder on s3
    output_foldername = str(time.time()) + "-" + randomString(10) + "/"
    return [
        internal_params,
        bucket,
        connection,
        output_foldername,
        aws_key_id,
        aws_access_key,
        db_password,
        instanceIds,
        enable_ec2_control,
    ]
