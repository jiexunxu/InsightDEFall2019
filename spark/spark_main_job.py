# Entry point for spark jobs

import sys

sys.path.insert(0, "./python")
import time
import process_cmd_params
import init
import ec2_manager
import select_images
import psycopg2_save_metadata
import opencv_transform_and_save_images
import spark_process_images
import spark_save_images
import notify_user
import subprocess
import upload_result_to_s3


def run_spark_job():
    # Builds the query string used by psycopg2_save_metadata.py
    def build_query_and_s3_image_files(imageids):
        s3_image_files = []
        query = "SELECT imageid,label,x_min,x_max,y_min,y_max FROM image_bboxes WHERE"
        for imageid in imageids:
            s3_image_files.append(
                "s3a://jiexunxu-open-image-dataset/train_data/" + imageid[0] + ".jpg"
            )
            query += " imageid='" + imageid[0] + "' OR"
        # remove last three redundant characters from query
        query = query[:-3]
        return [s3_image_files, query]

    # Obtain some initial parameters
    [user_email, user_param, user_selection, user_labels] = process_cmd_params.process()
    [
        internal_params,
        bucket,
        connection,
        output_foldername,
        aws_key,
        aws_access,
        db_password,
        instanceIds,
        enable_ec2_control,
    ] = init.init()
    if enable_ec2_control:
        ec2_manager.increment_requests(connection, instanceIds)
    # Find out the image names to be processed
    imageids = select_images.select(connection, user_selection)
    [s3_image_files, query] = build_query_and_s3_image_files(imageids)
    image_count = len(imageids)
    print("selected image count:" + str(image_count))
    # Process and save the updated bbox metadata
    start_time = time.time()
    psycopg2_save_metadata.save(connection, query, user_param, output_foldername)
    print("database execution time: " + str(time.time() - start_time))
    # Process and save the images themselves
    start_time = time.time()
    if image_count > internal_params[0]:
        # With a large scale request, save images individually to s3 and asks user to use aws cli to get them
        try:
            images_df = spark_process_images.transform(
                internal_params, s3_image_files, user_param
            )
            spark_save_images.save(
                internal_params,
                images_df,
                image_count,
                bucket,
                aws_key,
                aws_access,
                output_foldername,
            )
        except:
            pass
    else:
        # With a small scale request, save images locally and zip them up before uploading to s3
        command = "mkdir " + output_foldername
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        process.wait()
        for imageid in imageids:
            opencv_transform_and_save_images.process(
                bucket,
                imageid[0],
                output_foldername,
                user_param[0],
                user_param[1],
                user_param[2],
                user_param[3],
                user_param[4],
                user_param[5],
                user_param[6],
                user_param[7],
            )
        upload_result_to_s3.upload(bucket, output_foldername)
    print("saving images time: " + str(time.time() - start_time))
    # Job finished, notify the user
    notify_user.email_and_log(
        output_foldername,
        connection,
        user_email,
        user_selection,
        user_param,
        user_labels,
        image_count > internal_params[0],
    )
    if enable_ec2_control:
        ec2_manager.decrement_requests(connection, instanceIds)


run_spark_job()
