# This is a preprocessing function that reads train-annotations-human-imagelabels-boxable.csv, train-annotations-bbox.csv and class-descriptions.csv from S3, then create a new csv file on EC2 as a database for selecting images by user later.
# The database uses unique ImageID as keys, and for each key, it contains the following fields:
# ObjectCount, number of objects in this image
# IsManual, is 1 if all labels are google verified, 0 otherwise
# Image labels, a list of image level label names associated with this image
# This database is then output to a csv file called train-selection-database.csv

import boto3
import read_credentials

def combine():
    s3=boto3.resource('s3')
    bucket=s3.Bucket('jiexunxu-open-image-dataset') 
    # Read image level labels   
    lines=read_csv(bucket, 'train-annotations-human-imagelabels-boxable.csv')
    database={}
    line_count=0
    for row in lines:
        if line_count==0:
            line_count+=1
        else:
            key=row[0]
            if not key in database:
                database[key]=[0, 1]
            if row[1]=='crowdsource-verification' or row[1]=='machine':
                database[key][1]=0
            database[key].append(row[2])
    # Read bbox count
    lines=read_csv(bucket, 'train-annotations-bbox.csv')
    line_count=0
    for row in lines:
        if line_count==0:
            line_count+=1
        else:
            key=row[0]
			database[key][0]+=1
	# Output the combined database as csv file
	file=open('train-selection-database.csv', 'w')
	for key in database:
		entry=database[key]
		file.write("{},{},{}".format(key, entry[0], entry[1]))
		for i in range(2, len(entry)):
			file.write(","+entry[i])
		file.write('\n')
	file.close()

def read_csv(bucket, filename):
    obj=bucket.Object(filename)
    response=obj.get()
    lines=response['Body'].read().split()
    return lines
