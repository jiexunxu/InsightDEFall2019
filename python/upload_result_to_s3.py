# This function zips all the locally saved images into a single zip file, upload it to s3, and delete the local folder

import subprocess
from io import BytesIO


def upload(bucket, output_foldername):
    # Zipping the images
    print("Uploading images and meta-data to AWS S3 bucket")
    command = "zip -r result.zip " + output_foldername
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    process.wait()
    # Upload the zipped file, result.zip, to s3
    output_obj = bucket.Object("output_data/" + output_foldername + "result.zip")
    with open("result.zip", "rb") as f:
        output_obj.put(Body=f)
    # Remove the local image folder and result.zip
    command = "rm -r " + output_foldername
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    process.wait()
    command = "rm -r result.zip"
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    process.wait()
