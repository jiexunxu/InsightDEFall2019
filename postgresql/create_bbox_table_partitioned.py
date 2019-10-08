# Creates the image_bbox table with imageid as partitions to speed up queringimport boto3

import sys
sys.path.insert(0, './python')
import read_credentials
import boto3
import psycopg2

def main():
    [aws_key_id, aws_access_key, db_host, db_password, instanceIds, enable_ec2_control]=read_credentials.read()
    s3=boto3.resource('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_access_key)
    bucket=s3.Bucket('jiexunxu-open-image-dataset') 
    connection=psycopg2.connect(host = '127.0.0.1', database = 'imagedb', user = 'postgres', password = db_password)
    cursor=connection.cursor()
    cursor.execute('CREATE TABLE image_bbox_partitioned(imageid varchar(30) NOT NULL, source varchar(12) NOT NULL, label varchar(20) NOT NULL, confidence varchar(10), x_min float(10), x_max float(10), y_min float(10), y_max float(10), is_occ char, is_tru char, is_grp char, is_dep char, is_ins char, fhb_x_min float(10), fhb_x_max float(10), fhb_y_min float(10), fhb_y_max float(10), fvb_x_min float(10), fvb_x_max float(10), fvb_y_min float(10), fvb_y_max float(10), rb_x_min float(10), rb_x_max float(10), rb_y_min float(10), rb_y_max float(10)) PARTITIONED BY LIST(imageid)')
    cursor.execute('DELETE FROM image_bbox_partitioned')
    cursor.commit()
    lines=read_csv(bucket, 'train-annotations-bbox.csv')
    line_count=0
    percent=0
    for line in lines:
        if line_count>0:
            item=line.split(',')
            cursor.execute('INSERT INTO image_bbox (imageid, source, label, confidence, x_min, x_max, y_min, y_max, is_occ, is_tru, is_grp, is_dep, is_ins) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (item[0], item[1], item[2], item[3], float(item[4]), float(item[5]), float(item[6]), float(item[7]), item[8][0], item[9][0], item[10][0], item[11][0], item[12][0], 1-float(item[4]), 1-float(item[5]), float(item[6]), float(item[7]), float(item[4]), float(item[5]), 1-float(item[6]), 1-float(item[7]), 1-float(item[4]), 1-float(item[5]), 1-float(item[6]), 1-float(item[7])))
        line_count+=1
        percent=report_progress(connection, line_count, len(lines), percent)
        connection.commit()



main()
