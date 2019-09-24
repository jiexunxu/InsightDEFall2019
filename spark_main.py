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

def main(user_selection, user_param, user_email):
    [bucket, connection, output_foldername, history]=init.init()
    imageids=select_images.select(connection, user_selection[0], user_selection[1], user_selection[2], user_selection[3])
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
    for imageid in imageids:
        s3_image_files.append("s3a://jiexunxu-open-image-dataset/train_data/"+imageid[0]+".jpg")
        [bbox_descriptor, bbox_xy_enhanced]=process_single_image.process(bucket, connection, imageid[0], user_param, output_foldername)
        save_metadata.save(bucket, connection, imageid[0], bbox_descriptor, bbox_xy_enhanced, output_foldername, local_file1, local_file2)
    local_file1.close()
    local_file2.close()
    with open(local_file_name1, 'rb') as body:
        bucket.put_object(Key=output_foldername+'selected-train-annotations-human-imagelabels-boxable.csv', Body=body)
    with open(local_file_name2, 'rb') as body:
        bucket.put_object(Key=output_foldername+'selected-train-annotations-bbox.csv', Body=body)
                     
    print('start batch processing in spark')            
    spark = pyspark.sql.SparkSession.builder.appName("BatchImageProcessing").getOrCreate()
    images_df=spark.read.format("image").load(s3_image_files)    
    tr=(ImageTransformer().setOutputCol("transformed").resize(height=200, width=200).crop(0,0, height=160, width=120))
    result=tr.transform(images_df).select("transformed")
    result.write.format("avro").save("s3a://jiexunxu-open-image-dataset/"+output_foldername+"avro/")
    
    notify_user.email_and_log(output_foldername, user_email, user_selection, user_param)

#user_selection=[9, 10, 1, '/m/01_5g']
#user_param=[0, 0, 1, 0.3, 0.5, 1.8, 2.0, 3]
#user_email='gexelenor@4nextmail.com'
user_selection=[int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]]
user_param=[int(sys.argv[5]), int(sys.argv[6]), int(sys.argv[7]), float(sys.argv[8]), float(sys.argv[9]), float(sys.argv[10]), float(sys.argv[11]), int(sys.argv[12])]
user_email=sys.argv[13]
main(user_selection, user_param, user_email)
    
    
