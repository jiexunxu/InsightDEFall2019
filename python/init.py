# initializes s3 bucket and postgreSQL database

import boto3
import psycopg2
import read_credentials
import time

def init():
    [aws_key_id, aws_access_key, db_password]=read_credentials.read()
    s3=boto3.resource('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_access_key)
    bucket=s3.Bucket('jiexunxu-open-image-dataset') 
    connection=psycopg2.connect(host = 'ec2-3-230-4-222.compute-1.amazonaws.com', database = 'imagedb', user = 'postgres', password = db_password)
    output_foldername='output_data'+str(time.time())+'/'
    return [bucket, connection, db_password, output_foldername]    
