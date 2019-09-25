# Converts .avro file from spark to individual images

from fastavro import reader
from PIL import Image
import io
import numpy as np
import os
import subprocess
import sys

def batch_convert(bucket_name):
    download_avro_files(bucket_name)
    convert_avro_files()
    
def convert_avro_files():
    for avro_filename in os.listdir('.'):
        if avro_filename.endswith(".avro"):
            convert_single_avro_file(avro_filename)
    print("Finished extracting images from your .avro files on s3. The .avro files and image files are extracted in the current directory.")

def download_avro_files(bucket_name):
    command="aws s3 cp "+bucket_name+" ./ --recursive"
    process=subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    process.communicate()
        
def convert_single_avro_file(avro_filename):
    with open(avro_filename, 'rb') as fo:
        avro_reader = reader(fo)
        for record in avro_reader:
            fullname=record['transformed']['origin']
            name=os.path.basename(fullname)
            width=record['transformed']['width']
            height=record['transformed']['height']
            nChannels=record['transformed']['nChannels']
            image_bytes=record['transformed']['data']         
            if nChannels==3:
                array=np.frombuffer(image_bytes, dtype='uint8').reshape(height, width, 3)
                image=Image.fromarray(array, 'RGB')
                image.save(name)
            elif nChannels==1:
                array=np.frombuffer(image_bytes, dtype='uint8').reshape(height, width)
                array=np.repeat(array[:, :, np.newaxis], 3, axis=2)
                image=Image.fromarray(array, 'RGB')
                image.save(name)
    
batch_convert(sys.argv[1])
