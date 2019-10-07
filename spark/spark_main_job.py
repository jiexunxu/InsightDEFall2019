# Entry point for spark jobs

import sys
sys.path.insert(0, './python')
import time
import process_cmd_params
import init
import ec2_manager
import select_images
import spark_save_metadata
import psycopg2_save_metadata
import spark_process_images
import spark_save_images
import notify_user


def run_spark_job(): 
    def build_query_and_s3_image_files(imageids):
        s3_image_files=[]
        query="SELECT * from image_bbox WHERE"
        for imageid in imageids:
            s3_image_files.append("s3a://jiexunxu-open-image-dataset/train_data/"+imageid[0]+".jpg")
            query+=" imageid='"+imageid[0]+"' OR"
        # remove last three redundant characters from query
        query=query[:-3]
        return [s3_image_files, query]

    [user_email, user_param, user_selection, user_labels]=process_cmd_params.process()
    [internal_params, bucket, connection, output_foldername, aws_key, aws_access, db_password, instanceIds, enable_ec2_control]=init.init()
    if enable_ec2_control:
        ec2_manager.increment_requests(connection, instanceIds)
    imageids=select_images.select(connection, user_selection)
    [s3_image_files, query]=build_query_and_s3_image_files(imageids)
    image_count=len(imageids)
    print('selected image count:'+str(image_count))
    
    start_time=time.time()
    if image_count>internal_params[1]:
        spark_save_metadata.save(output_foldername, query, db_password, user_param)
    else:
        psycopg2_save_metadata.save(connection, query, user_param, output_foldername)
    print("database execution time: "+str(time.time()-start_time))

    print("start batch processing in spark")       
    start_time=time.time()
    images_df=spark_process_images.transform(internal_params, s3_image_files, user_param)
    is_large_scale_image_save=spark_save_images.save(internal_params, images_df, image_count, bucket, aws_key, aws_access, output_foldername)
    print("saving images time: "+str(time.time()-start_time))
    
    notify_user.email_and_log(output_foldername, connection, user_email, user_selection, user_param, user_labels, is_large_scale_image_save)
    if enable_ec2_control:
        ec2_manager.decrement_requests(connection, instanceIds)


run_spark_job()
