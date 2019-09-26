import subprocess
from io import BytesIO

def upload(bucket, output_foldername):
    print("Uploading images and meta-data to AWS S3 bucket")
    command="zip -r result.zip "+output_foldername
    process=subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    process.wait()
    output_obj=bucket.Object('output_data/'+output_foldername+"result.zip")
    with open("result.zip", "rb") as f:
    	output_obj.put(Body=f)
    command="rm -r "+output_foldername
    process=subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    process.wait()
    command="rm -r result.zip"
    process=subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    process.wait()

    
