# Save the processed images to s3. Save format differs depending on how many images are processed

import boto3
import avro_to_images
import upload_result_to_s3
import numpy as np
from PIL import Image
from io import ByteIO

def save(internal_params, images_df, image_count, aws_key, aws_access, output_foldername):
    if image_count>internal_params[1]:
        # Dump out avro files to s3 for user to download and extract image on their end. A script will be provided to do this
        print("You have over 1000 images to process, saving .avro to s3")
        images_df.write.format("avro").mode("overwrite").save("s3a://jiexunxu-open-image-dataset/output_data/"+output_foldername+"avro-format-images/")
    elif image_count>internal_params[0]:
        # Save individual images to s3 bucket
        print("You have over 100 images to process, saving individual images to s3")
        images_df.rdd.foreach(lambda record : save_individual_images_to_s3(record, aws_key, aws_access, output_foldername))
    else:
        # Extract and save images locally, zip it up, send it to s3, and delete local data afterwards
        print("You have less than 100 images to process, zipping up everything and saving to s3")
        avro_path="./avro_output/"
        images_df.write.format("avro").mode("overwrite").save(avro_path)
        avro_to_images.convert(avro_path, output_foldername)
        upload_result_to_s3.upload(bucket, output_foldername)    
    
    def save_individual_images_to_s3(record, aws_key, aws_access, output_foldername):
        # Need to re-initialize a boto3 connection
        s3=boto3.resource('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_access)
        bucket=s3.Bucket('jiexunxu-open-image-dataset')
        tags=["fhb_", "fvb_", "rb_", "sb_", "cb_"]
        for i in range(len(tags)):
            if tags[i] in record:
                tag=tags[i]
                # Get info from individual images from the schema
                fullname=record[tag]['origin']
                name=os.path.basename(fullname)
                width=record[tag]['width']
                height=record[tag]['height']
                nChannels=record[tag]['nChannels']
                image_bytes=record[tag]['data']      
                if nChannels==1 or nChannels==3:   
                    if nChannels==3:
                        array=np.frombuffer(image_bytes, dtype='uint8').reshape(height, width, 3)
                        image=Image.fromarray(array, 'RGB')                        
                    elif nChannels==1:
                        array=np.frombuffer(image_bytes, dtype='uint8').reshape(height, width)
                        array=np.repeat(array[:, :, np.newaxis], 3, axis=2)
                    # Save the image to s3
                    image=Image.fromarray(array, 'RGB')
                    output_obj=bucket.Object("output_data/"+output_foldername+tag+name)
                    file_stream=BytesIO()
                    image.save(file_stream, format='jpeg')
                    output_obj.put(Body=file_stream.getvalue())