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
from pyspark.sql.functions import concat, col, lit
import copy
import s3fs

def main(user_selection, user_param, user_email):
    use_spark_db=False
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
    if use_spark_db:
        dbspark=SparkSession.builder.appName("SparkDBSession").getOrCreate() 
        bbox_df_flip_horizontal=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", "SELECT * from image_bbox"+query).option("user", "postgres").option("password", "qwerty").load()
        bbox_df_flip_vertical=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", "SELECT * from image_bbox"+query).option("user", "postgres").option("password", "qwerty").load()
        bbox_df_rotate=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", "SELECT * from image_bbox"+query).option("user", "postgres").option("password", "qwerty").load()
        bbox_df_scale=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", "SELECT * from image_bbox"+query).option("user", "postgres").option("password", "qwerty").load()
        bbox_df_crop=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", "SELECT * from image_bbox"+query).option("user", "postgres").option("password", "qwerty").load()
        output_s3_name="s3a://jiexunxu-open-image-dataset/output_data/"+output_foldername+"selected-train-annotations-bbox"
        bbox_df_flip_horizontal.withColumn("x_min", 1-bbox_df_flip_horizontal.x_min).withColumn("x_max", 1-bbox_df_flip_horizontal.x_max).withColumn("imageid", concat(lit("fhb_"), bbox_df_flip_horizontal.imageid)).write.csv(output_s3_name+"-flip-horizontal")
        bbox_df_flip_vertical.withColumn("y_min", 1-bbox_df_flip_vertical.y_min).withColumn("y_max", 1-bbox_df_flip_vertical.y_max).withColumn("imageid", concat(lit("fvb_"), bbox_df_flip_vertical.imageid)).write.csv(output_s3_name+"-flip-vertical")
        bbox_df_rotate.withColumn("x_min", 1-bbox_df_rotate.x_min).withColumn("x_max", 1-bbox_df_rotate.x_max).withColumn("y_min", 1-bbox_df_rotate.y_min).withColumn("y_max", 1-bbox_df_rotate.y_max).withColumn("imageid", concat(lit("rb_"), bbox_df_rotate.imageid)).write.csv(output_s3_name+"-rotate")
        bbox_df_scale.withColumn("x_min", (bbox_df_scale.x_min-(user_param[3]-1)/2)/(2-user_param[3])).withColumn("x_max", (bbox_df_scale.x_max-(user_param[3]-1)/2)/(2-user_param[3])).withColumn("y_min", (bbox_df_scale.y_min-(user_param[3]-1)/2)/(2-user_param[3])).withColumn("y_max", (bbox_df_scale.y_max-(user_param[3]-1)/2)/(2-user_param[3])).withColumn("imageid", concat(lit("sb_"), bbox_df_scale.imageid)).filter((bbox_df_scale.x_min>=float(0.0)) & (bbox_df_scale.x_max<=float(1.0)) & (bbox_df_scale.y_min>=float(0.0)) & (bbox_df_scale.y_max<=float(1.0))).write.csv(output_s3_name+"-scale")
        bbox_df_crop.withColumn("x_min", (bbox_df_crop.x_min-user_param[4])/(user_param[5]-user_param[4])).withColumn("x_max", (bbox_df_crop.x_max-user_param[4])/(user_param[5]-user_param[4])).withColumn("y_min", (bbox_df_crop.y_min-user_param[6])/(user_param[7]-user_param[6])).withColumn("y_max", (bbox_df_crop.y_max-user_param[6])/(user_param[7]-user_param[6])).withColumn("imageid", concat(lit("cb_"), bbox_df_crop.imageid)).filter((bbox_df_crop.x_min>=float(0.0)) & (bbox_df_crop.x_max<=float(1.0)) & (bbox_df_crop.y_min>=float(0.0)) & (bbox_df_crop.y_max<=float(1.0))).write.csv(output_s3_name+"-crop")
       # bbox_df_flip_horizontal.union(bbox_df_flip_vertical).union(bbox_df_rotate).union(bbox_df_scale).union(bbox_df_crop).coalesce(1).write.csv("s3a://jiexunxu-open-image-dataset/output_data/"+output_foldername+"selected-train-annotations-bbox.csv")
 #   with open(local_file_name1, 'rb') as body:
  #      bucket.put_object(Key='output/'+output_foldername+'selected-train-annotations-human-imagelabels-boxable.csv', Body=body)
  #  with open(local_file_name2, 'rb') as body:
  #      bucket.put_object(Key='output/'+output_foldername+'selected-train-annotations-bbox.csv', Body=body)
    else:
        cursor=connection.cursor()
        cursor.execute("SELECT * from image_bbox"+query)
        bbox_descriptors=cursor.fetchall()
        num_of_rows=len(bbox_descriptors)
        bbox_flip_horizontal=np.zeros((num_of_rows, 4))
        bbox_flip_horizontal[:, 0]=[row[4] for row in bbox_descriptors]
        bbox_flip_horizontal[:, 1]=[row[5] for row in bbox_descriptors]
        bbox_flip_horizontal[:, 2]=[row[6] for row in bbox_descriptors]
        bbox_flip_horizontal[:, 3]=[row[7] for row in bbox_descriptors]
        bbox_flip_vertical=np.copy(bbox_flip_horizontal)
        bbox_rotate=np.copy(bbox_flip_horizontal)
        bbox_scale=np.copy(bbox_flip_horizontal)
        bbox_crop=np.copy(bbox_flip_horizontal)
        
        bbox_flip_horizontal[:, 0]=1-bbox_flip_horizontal[:, 0]
        bbox_flip_horizontal[:, 1]=1-bbox_flip_horizontal[:, 1]
        
        bbox_flip_vertical[:, 2]=1-bbox_flip_vertical[:, 2]
        bbox_flip_vertical[:, 3]=1-bbox_flip_vertical[:, 3]
        
        bbox_rotate[:, 0]=1-bbox_rotate[:, 0]
        bbox_rotate[:, 1]=1-bbox_rotate[:, 1]
        bbox_rotate[:, 2]=1-bbox_rotate[:, 2]
        bbox_rotate[:, 3]=1-bbox_rotate[:, 3]
        
        bbox_scale[:, 0]=bbox_scale[:, 0]-(user_param[3]-1)/2
        bbox_scale[:, 1]=bbox_scale[:, 1]-(user_param[3]-1)/2
        bbox_scale[:, 2]=bbox_scale[:, 2]-(user_param[3]-1)/2
        bbox_scale[:, 3]=bbox_scale[:, 3]-(user_param[3]-1)/2
        bbox_scale[np.logical_or(np.logical_or(bbox_scale[:, 0]<0, bbox_scale[:, 1]>1), np.logical_or(bbox_scale[:, 2]<0, bbox_scale[:, 3]>1)), 0]=-1
        
        bbox_crop[:, 0]=(bbox_crop[:, 0]--user_param[4])/(user_param[5]-user_param[4])
        bbox_crop[:, 1]=(bbox_crop[:, 1]--user_param[4])/(user_param[5]-user_param[4])
        bbox_crop[:, 2]=(bbox_crop[:, 2]--user_param[6])/(user_param[7]-user_param[6])
        bbox_crop[:, 3]=(bbox_crop[:, 3]--user_param[6])/(user_param[7]-user_param[6])
        bbox_crop[np.logical_or(np.logical_or(bbox_crop[:, 0]<0, bbox_crop[:, 1]>1), np.logical_or(bbox_crop[:, 2]<0, bbox_crop[:, 3]>1)), 0]=-1
        
        s3=s3fs.S3FileSystem(anon=False)
        with s3.open("jiexunxu-open-image-dataset/output_data/"+output_foldername+"selected-train-annotations-bbox.csv", "w") as f:
            for i in range(len(bbox_descriptors)):
                row=bbox_descriptors[i]
                f.write("fhb_"+row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(bbox_flip_horizontal[i][0])+","+str(bbox_flip_horizontal[i][1])+","+str(bbox_flip_horizontal[i][2])+","+str(bbox_flip_horizontal[i][3])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+"\n")
            for i in range(len(bbox_descriptors)):
                row=bbox_descriptors[i]
                f.write("fvb_"+row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(bbox_flip_vertical[i][0])+","+str(bbox_flip_vertical[i][1])+","+str(bbox_flip_vertical[i][2])+","+str(bbox_flip_vertical[i][3])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+"\n")
            for i in range(len(bbox_descriptors)):
                row=bbox_descriptors[i]
                f.write("rb_"+row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(bbox_rotate[i][0])+","+str(bbox_rotate[i][1])+","+str(bbox_rotate[i][2])+","+str(bbox_rotate[i][3])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+"\n")
            for i in range(len(bbox_descriptors)):
                row=bbox_descriptors[i]
                if bbox_scale[i][0]>=0:
                    f.write("sb_"+row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(bbox_scale[i][0])+","+str(bbox_scale[i][1])+","+str(bbox_scale[i][2])+","+str(bbox_scale[i][3])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+"\n")
            for i in range(len(bbox_descriptors)):
                row=bbox_descriptors[i]
                if bbox_crop[i][0]>=0:
                    f.write("cb_"+row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(bbox_crop[i][0])+","+str(bbox_crop[i][1])+","+str(bbox_crop[i][2])+","+str(bbox_crop[i][3])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+"\n")

                 
    print("start batch processing in spark")            
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
    
    
