# This python script reads from class-descriptions-boxable.csv, train-annotations-bbox.csv, train-annotations-human-imagelabels-boxable.csv and train-selection-database.csv on AWS S3, and connects to the imagedb database locally. It then puts the contents of these four files into the coresponding entries into the databases
#
# This script is not part of the real time BOSA system. It is used to preprocess metadata

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
    print('start insertion into image_bboxes')
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

    # Use imageid as PRIMARY KEY for faster query access. Concatenates all bbox metadata into an array
    cursor.execute('DELETE FROM image_bboxes')
    connection.commit()
    lines=read_csv(bucket, 'train-annotations-bbox.csv')
    line_count=0
    percent=0
    labels=[]
    x_mins=[]
    x_maxs=[]
    y_mins=[]
    y_maxs=[]
    x_mins_1=[]
    x_maxs_1=[]
    y_mins_1=[]
    y_maxs_1=[]
    for line in lines:
        if line_count>0:
            item=line.split(',')
            imageid=item[0]
            if imageid!=last_imageid:
                cursor.execute('INSERT INTO image_bboxes (imageid, label, x_min, x_max, y_min, y_max, fhb_x_min, fhb_x_max, fhb_y_min, fhb_y_max, fvb_x_min, fvb_x_max, fvb_y_min, fvb_y_max, rb_x_min, rb_x_max, rb_y_min, rb_y_max) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (imageid, labels, x_mins, x_maxs, y_mins, y_maxs, x_mins_1, x_maxs_1, y_mins, y_maxs, x_mins, x_maxs, y_mins_1, y_maxs_1, x_mins_1, x_maxs_1, y_mins_1, y_maxs_1))
                connection.commit()
                last_imageid=imageid
                labels=[]
                x_mins=[]
                x_maxs=[]
                y_mins=[]
                y_maxs=[]
                x_mins_1=[]
                x_maxs_1=[]
                y_mins_1=[]
                y_maxs_1=[]
            labels.append(label_dict[item[2]])
            x_mins.append(float(item[4]))
            x_maxs.append(float(item[5]))
            y_mins.append(float(item[6]))
            y_maxs.append(float(item[7]))
            x_mins_1.append(1-float(item[4]))
            x_maxs_1.append(1-float(item[5]))
            y_mins_1.append(1-float(item[6]))
            y_maxs_1.append(1-float(item[7]))
        line_count+=1
        percent=report_progress(connection, line_count, len(lines), percent)
        connection.commit()
    cursor.execute('INSERT INTO image_bboxes (imageid, label, x_min, x_max, y_min, y_max, fhb_x_min, fhb_x_max, fhb_y_min, fhb_y_max, fvb_x_min, fvb_x_max, fvb_y_min, fvb_y_max, rb_x_min, rb_x_max, rb_y_min, rb_y_max) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (imageid, labels, x_mins, x_maxs, y_mins, y_maxs, x_mins_1, x_maxs_1, y_mins, y_maxs, x_mins, x_maxs, y_mins_1, y_maxs_1, x_mins_1, x_maxs_1, y_mins_1, y_maxs_1))
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
