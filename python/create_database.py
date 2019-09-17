# This python script reads from class-descriptions-boxable.csv, train-annotations-bbox.csv, train-annotations-human-imagelabels-boxable.csv and train-selection-database.csv on AWS S3, and connects to the imagedb database locally. It then puts the contents of these four files into the coresponding entries into the databases

import boto3
import psycopg2
import read_credentials

def main():
    [aws_key_id, aws_access_key, db_password]=read_credentials.read()
    s3=boto3.resource('s3')
    bucket=s3.Bucket('jiexunxu-open-image-dataset', aws_key_id, aws_access_key) 
    connection=psycopg2.connect(host = '127.0.0.1', database = 'imagedb', user = 'postgres', password = db_password)
    cursor=connection.cursor()
    # Read image level labels   
    lines=read_csv(bucket, 'class-descriptions-boxable.csv')
    line_count=0    
    for row in lines:
        cursor.execute('''INSERT INTO label_names (label, name) VALUES ({}, {})'''.format(row[0], row[1]))
    cursor.close()
    connection.close()

def read_csv(bucket, filename):
    obj=bucket.Object(filename)
    response=obj.get()
    lines=response['Body'].read().split()
    return lines

main()
