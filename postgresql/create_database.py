# This python script reads from class-descriptions-boxable.csv, train-annotations-bbox.csv, train-annotations-human-imagelabels-boxable.csv and train-selection-database.csv on AWS S3, and connects to the imagedb database locally. It then puts the contents of these four files into the coresponding entries into the databases

import boto3
import psycopg2

def main():
    [aws_key_id, aws_access_key, db_password]=read_credentials.read()
    s3=boto3.resource('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_access_key)
    bucket=s3.Bucket('jiexunxu-open-image-dataset') 
    connection=psycopg2.connect(host = '127.0.0.1', database = 'imagedb', user = 'postgres', password = db_password)
    cursor=connection.cursor()

    print('start insertion into label_names')
    insert_into_label_names(bucket, connection, cursor) 
    print('start insertion into image_labels')
    insert_into_image_labels(bucket, connection, cursor)
    print('start insertion into image_bbox')
    insert_into_image_bbox(bucket, connection, cursor)
    print('start insertion into image_selection')
    insert_into_image_selection(bucket, connection, cursor)
    cursor.close()
    connection.close()

def insert_into_label_names(bucket, connection, cursor):
    lines=read_csv(bucket, 'class-descriptions-boxable.csv')   
    cursor.execute('DELETE FROM label_names')
    for line in lines:
        item=line.split(',')
        cursor.execute('INSERT INTO label_names (label, name) VALUES (%s, %s)', (item[0], item[1]))
    connection.commit()

def insert_into_image_labels(bucket, connection, cursor):
    lines=read_csv(bucket, 'train-annotations-human-imagelabels-boxable.csv')
    line_count=0
    percent=0
    cursor.execute('DELETE FROM image_labels')
    for line in lines:
        if line_count>0:        
            item=line.split(',')
            cursor.execute('INSERT INTO image_labels (imageid, source, label, confidence) VALUES (%s, %s, %s, %s)', (item[0], item[1], item[2], item[3]))
        line_count+=1    
        percent=report_progress(connection, line_count, len(lines), percent)
    connection.commit()

def insert_into_image_bbox(bucket, connection, cursor):
    cursor.execute('DELETE FROM image_bbox')
    lines=read_csv(bucket, 'train-annotations-bbox.csv')
    line_count=0
    percent=0
    for line in lines:
        if line_count>0:
            item=line.split(',')
            cursor.execute('INSERT INTO image_bbox (imageid, source, label, confidence, x_min, x_max, y_min, y_max, is_occ, is_tru, is_grp, is_dep, is_ins) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (item[0], item[1], item[2], item[3], float(item[4]), float(item[5]), float(item[6]), float(item[7]), item[8][0], item[9][0], item[10][0], item[11][0], item[12][0]))
        line_count+=1
        percent=report_progress(connection, line_count, len(lines), percent)
    connection.commit()


def insert_into_image_selection(bucket, connection, cursor):
    lines=read_csv(bucket, 'train-selection-database.csv')
    cursor.execute('DELETE FROM image_selection')
    line_count=0
    percent=0
    for line in lines:
        item=line.split(',')
        cursor.execute('INSERT INTO image_selection (imageid, obj_count, is_google, label) VALUES (%s, %s, %s, %s)', (item[0], int(item[1]), item[2], item[3]))
        line_count+=1
        percent=report_progress(connection, line_count, len(lines), percent)
    connection.commit()
    
def read_csv(bucket, filename):
    obj=bucket.Object(filename)
    response=obj.get()
    lines=response['Body'].read().decode('utf-8').splitlines()
    return lines

def report_progress(connection, line_count, total_lines, percent):
    if line_count>=int(total_lines/100*percent):
        connection.commit()
        print('finished {}%'.format(percent))
        percent+=1
    return percent

main()
