# Converts .avro file from spark to individual images

from fastavro import reader
from PIL import Image
import io
import numpy as np
import os
import subprocess
import sys
    
def convert(avro_path, output_foldername):
    print("Extracting images from avro files from spark")
    command="mkdir "+output_foldername
    process=subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    process.wait()

    for avro_filename in os.listdir(avro_path):
        if avro_filename.endswith(".avro"):
            convert_single_avro_file(avro_path+avro_filename, "./"+output_foldername)
    command="rm -r "+avro_path
    process=subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    process.wait()

def convert_single_avro_file(avro_filename, image_folder):
    output_record_tags=["fhb_transform", "fvb_transform", "rb_transform"]
    with open(avro_filename, 'rb') as fo:
        avro_reader = reader(fo)
        for record in avro_reader:
            for i in range(len(output_record_tags)):
                tag=output_record_tags[i]
                fullname=record[tag]['origin']
                name=os.path.basename(fullname)
                width=record[tag]['width']
                height=record[tag]['height']
                nChannels=record[tag]['nChannels']
                image_bytes=record[tag]['data']         
                if nChannels==3:
                    array=np.frombuffer(image_bytes, dtype='uint8').reshape(height, width, 3)
                    image=Image.fromarray(array, 'RGB')
                    image.save(image_folder+tag+name)
                elif nChannels==1:
                    array=np.frombuffer(image_bytes, dtype='uint8').reshape(height, width)
                    array=np.repeat(array[:, :, np.newaxis], 3, axis=2)
                    image=Image.fromarray(array, 'RGB')
                    image.save(image_folder+tag+name)
