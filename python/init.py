# initializes s3 bucket and postgreSQL database

import boto3
import psycopg2
import read_credentials
import time

def init():
    [aws_key_id, aws_access_key, db_host, db_password]=read_credentials.read()
    s3=boto3.resource('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_access_key)
    bucket=s3.Bucket('jiexunxu-open-image-dataset')
    connection=psycopg2.connect(host = db_host, database = 'imagedb', user = 'postgres', password = db_password)
    output_foldername=str(time.time())+'/'
    user_history={}
    with open("boise_log.txt", "r") as f:
        for line in f:
            items=line.split(',')
            user_email=items[0]
            if not user_email in user_history:
                user_history[user_email]=[]
            for i in range(1, len(items)):
                user_history[user_email].append(items[i])
    return [bucket, connection, output_foldername, user_history]    

