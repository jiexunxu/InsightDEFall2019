# initializes s3 bucket and postgreSQL database

import boto3
import psycopg2
import read_credentials
import time

def init():
    # If there are more than internal_params[1] images to process, dump them as .avro files to s3
    # If there are between internal_params[0] and internal_params[1] images to process, save them as individual files on s3
    # If there are less than internal_params[0] images to process, save them locally and upload to s3 as a zip
    # If there are more than internal_params[2] images to process, use spark_save_metadata, else use psycopg2_save_metadata
    internal_params=[50, 10000, 2000]
    [aws_key_id, aws_access_key, db_host, db_password]=read_credentials.read()
    s3=boto3.resource('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_access_key)
    bucket=s3.Bucket('jiexunxu-open-image-dataset')
    connection=psycopg2.connect(host = db_host, database = 'imagedb', user = 'postgres', password = db_password)
    output_foldername=str(time.time())+'/'
    return [internal_params, bucket, connection, output_foldername, aws_key_id, aws_access_key, db_password]    

