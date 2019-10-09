# Save the processed images to s3. Save format differs depending on how many images are processed

import boto3
import os
import upload_result_to_s3
import numpy as np
from PIL import Image
from io import BytesIO
import subprocess


def save(
    internal_params,
    images_df,
    image_count,
    bucket,
    aws_key,
    aws_access,
    output_foldername,
):
    def save_individual_images(record, aws_key, aws_access, output_foldername):
        # Need to re-initialize a boto3 connection for each slave node
        s3 = boto3.resource(
            "s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_access
        )
        bucket = s3.Bucket("jiexunxu-open-image-dataset")
        tags = ["fhb_", "fvb_", "rb_", "sb_", "cb_"]
        for i in range(len(tags)):
            if tags[i] in record:
                tag = tags[i]
                # Get info from individual images from the image df schema
                fullname = record[tag]["origin"]
                name = os.path.basename(fullname)
                width = record[tag]["width"]
                height = record[tag]["height"]
                nChannels = record[tag]["nChannels"]
                image_bytes = record[tag]["data"]
                if nChannels == 1 or nChannels == 3:
                    if nChannels == 3:
                        array = np.frombuffer(image_bytes, dtype="uint8").reshape(
                            height, width, 3
                        )
                        image = Image.fromarray(array, "RGB")
                    elif nChannels == 1:
                        array = np.frombuffer(image_bytes, dtype="uint8").reshape(
                            height, width
                        )
                        array = np.repeat(array[:, :, np.newaxis], 3, axis=2)
                    # Save the image to s3
                    image = Image.fromarray(array, "RGB")
                    output_obj = bucket.Object(
                        "output_data/" + output_foldername + tag + name
                    )
                    file_stream = BytesIO()
                    image.save(file_stream, format="jpeg")
                    output_obj.put(Body=file_stream.getvalue())

    # Save individual images to s3 bucket
    print(
        "You have over "
        + str(internal_params[0])
        + " images to process, saving individual images to s3"
    )
    # Convert the image df to an RDD, iterate over each element, extract the images, and save them to s3
    images_df.rdd.foreach(
        lambda record: save_individual_images(
            record, aws_key, aws_access, output_foldername
        )
    )
