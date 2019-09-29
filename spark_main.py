# Implements batch image transforms on given images from s3

import sys
sys.path.insert(0, './python')
sys.path.insert(0, './spark')
sys.path.insert(0, './postgresql')
sys.path.insert(0, './flask')
import init
import select_images
import process_single_image
import save_metadata
import notify_user
import mmlspark
import numpy as np
from mmlspark.opencv import toNDArray
from mmlspark.opencv import ImageTransformer
from mmlspark.io import *
import sys
import avro_to_images
import upload_result_to_s3
from PIL import Image
import os
from io import BytesIO
import boto3
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext



def main(user_selection, user_param, user_email):
    [bucket, connection, output_foldername, aws_key, aws_access]=init.init()
    imageids=select_images.select(connection, user_selection)
    local_file_name1="tmp1.tmpdump~"
    with open(local_file_name1, 'w') as f:
        f.write("ImageID,Source,LabelName,Confidence\n")
    local_file1=open(local_file_name1, 'a')
    local_file_name2="tmp2.tmpdump~"
    with open(local_file_name2, 'w') as f:
        f.write("ImageID,Source,LabelName,Confidence,XMin,XMax,YMin,YMax,IsOccluded,IsTruncated,IsGroupOf,IsDepiction,IsInside\n")
    local_file2=open(local_file_name2, 'a')
    print('selected image count:'+str(len(imageids)))
    
    s3_image_files=[]
    query=" WHERE imageid='dummyvalue'"
    for imageid in imageids:
        s3_image_files.append("s3a://jiexunxu-open-image-dataset/train_data/"+imageid[0]+".jpg")
        query+=" OR imageid='"+imageid[0]+"'"
#        [bbox_descriptor, bbox_xy_enhanced]=process_single_image.process(bucket, connection, imageid[0], user_param, output_foldername)
#        save_metadata.save(bucket, connection, imageid[0], bbox_descriptor, bbox_xy_enhanced, output_foldername, local_file1, local_file2)
    local_file1.close()
    local_file2.close()
    dbspark=SparkSession.builder.appName("SparkDBSession").getOrCreate() 
    bbox_df_flip_horizontal=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", "SELECT * from image_bbox"+query).option("user", "postgres").option("password", "qwerty").load()
    bbox_df_flip_horizontal.withColumn("x_min", 1-bbox_df_flip_horizontal.x_min)
    bbox_df_flip_horizontal.withColumn("x_max", 1-bbox_df_flip_horizontal.x_max)
    bbox_df_flip_horizontal.withColumn("imageid", "fhb_"+bbox_df_flip_horizontal.imageid)
    
    bbox_df_flip_vertical=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", "SELECT * from image_bbox"+query).option("user", "postgres").option("password", "qwerty").load()
    bbox_df_flip_vertical.withColumn("y_min", 1-bbox_df_flip_vertical.y_min)
    bbox_df_flip_vertical.withColumn("y_max", 1-bbox_df_flip_vertical.y_max)
    
    bbox_df_rotate=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", "SELECT * from image_bbox"+query).option("user", "postgres").option("password", "qwerty").load()
    bbox_df_rotate.withColumn("x_min", 1-bbox_df_rotate.x_min)
    bbox_df_rotate.withColumn("x_max", 1-bbox_df_rotate.x_max)
    bbox_df_rotate.withColumn("y_min", 1-bbox_df_rotate.y_min)
    bbox_df_rotate.withColumn("y_max", 1-bbox_df_rotate.y_max)
    
    all_bbox=bbox_df_flip_horizontal.union(bbox_df_flip_vertical)
    all_bbox=all_bbox.union(bbox_df_rotate)
    all_bbox.coalesce(1).write.csv("s3a://jiexunxu-open-image-dataset/output_data/"+output_foldername+"selected-train-annotations-bbox.csv")
 #   with open(local_file_name1, 'rb') as body:
  #      bucket.put_object(Key='output/'+output_foldername+'selected-train-annotations-human-imagelabels-boxable.csv', Body=body)
  #  with open(local_file_name2, 'rb') as body:
  #      bucket.put_object(Key='output/'+output_foldername+'selected-train-annotations-bbox.csv', Body=body)
                     
    print('start batch processing in spark')            
    spark = pyspark.sql.SparkSession.builder.appName("BatchImageProcessing").getOrCreate()
    images_df=spark.read.format("image").load(s3_image_files) 

    L=user_param[0]
    flip_horizontal_blur_trans=(ImageTransformer().setOutputCol("fhb_").resize(L, L).flip(flipCode=1).gaussianKernel(user_param[1], user_param[2]))
    flip_vertical_blur_trans=(ImageTransformer().setOutputCol("fvb_").resize(L, L).flip(flipCode=0).gaussianKernel(user_param[1], user_param[2]))
    rotate_blur_trans=(ImageTransformer().setOutputCol("rb_").resize(L, L).flip(flipCode=0).flip(flipCode=1).gaussianKernel(user_param[1], user_param[2]))
    scale_blur_trans=(ImageTransformer().setOutputCol("sb_").resize(int(L*user_param[3]), int(L*user_param[3])).crop(int(L*(user_param[3]-1)/2), int(L*(user_param[3]-1)/2), L, L).gaussianKernel(user_param[1], user_param[2]))
    crop_blur_trans=(ImageTransformer().setOutputCol("cb_").resize(L, L).crop(int(L*user_param[4]), int(L*user_param[6]), int(L*(user_param[7]-user_param[6])), int(L*(user_param[5]-user_param[4]))).resize(L, L).gaussianKernel(user_param[1], user_param[2]))
    result=flip_horizontal_blur_trans.transform(images_df)
    result=flip_vertical_blur_trans.transform(result)
    result=rotate_blur_trans.transform(result)
    result=scale_blur_trans.transform(result)
    result=crop_blur_trans.transform(result)
    if len(imageids)>1000:
        print("You have over 100 images to process, saving .avro to s3")
        result.write.format("avro").mode("overwrite").save("s3a://jiexunxu-open-image-dataset/output_data/"+output_foldername+"images.avro/")
    elif len(imageids)>1:
        result=result.rdd
        result.foreach(lambda record : saveImages(record, aws_key, aws_access, output_foldername))
    else:
        avro_path="./avro_output/"
        result.write.format("avro").mode("overwrite").save(avro_path)
        avro_to_images.convert(avro_path, output_foldername)
        upload_result_to_s3.upload(bucket, output_foldername)    
        notify_user.email_and_log(output_foldername, user_email, user_selection, user_param)

#user_selection=[9, 10, 1, '/m/01_5g']
# user param: desired image size, gaussian blur apperture size (odd integer), gaussian blur sigma, scale factor, crop xmin, crop xmax, crop ymin, crop ymax
#user_param=[0, 0, 1, 0.3, 0.5, 1.8, 2.0, 3]
#user_email='gexelenor@4nextmail.com'

def saveImages(record, aws_key, aws_access, output_foldername):
    s3=boto3.resource('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_access)
    bucket=s3.Bucket('jiexunxu-open-image-dataset')
    tags=["fhb_", "fvb_", "rb_", "sb_", "cb_"]
    for i in range(len(tags)):
        if tags[i] in record:
            tag=tags[i]
            fullname=record[tag]['origin']
            name=os.path.basename(fullname)
            width=record[tag]['width']
            height=record[tag]['height']
            nChannels=record[tag]['nChannels']
            image_bytes=record[tag]['data']         
            if nChannels==3:
                array=np.frombuffer(image_bytes, dtype='uint8').reshape(height, width, 3)
                image=Image.fromarray(array, 'RGB')
                output_obj=bucket.Object("output_data/"+output_foldername+tag+name)
                file_stream=BytesIO()
                image.save(file_stream, format='jpeg')
                output_obj.put(Body=file_stream.getvalue())
            elif nChannels==1:
                array=np.frombuffer(image_bytes, dtype='uint8').reshape(height, width)
                array=np.repeat(array[:, :, np.newaxis], 3, axis=2)
                image=Image.fromarray(array, 'RGB')
                output_obj=bucket.Object("output_data/"+output_foldername+tag+name)
                file_stream=BytesIO()
                image.save(file_stream, format='jpeg')
                output_obj.put(Body=file_stream.getvalue())

user_email=sys.argv[1]
user_param=[int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5]), float(sys.argv[6]), float(sys.argv[7]), float(sys.argv[8]), float(sys.argv[9])]
user_selection=[int(sys.argv[10]), int(sys.argv[11]), int(sys.argv[12])]
for i in range(13, len(sys.argv)):
    user_selection.append(sys.argv[i])
main(user_selection, user_param, user_email)
    
    
