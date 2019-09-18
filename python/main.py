# The main pipeline

import init
import select_images
import enhance_single_image
import save_metadata

def start():
    user_selection=[3, 0, '/m/01_5g']
    user_param=[0, 1, 0.3, 0.5, 1.8, 2.0, 3]
    [bucket, connection, db_password, output_foldername]=init.init()
    imageids=select_images.select(connection, user_selection[0], user_selection[1], user_selection[2])
    # later in spark we will divide imageids into 4 parts and assign each part to a EC2 instance to do the job below
    all_bbox_descriptor=[]
    for imageid in imageids:
        bbox_descriptor=enhance_single_image.enhance(bucket, connection, imageid, user_param, output_foldername)
        all_bbox_descriptor.append(bbox_descriptor)
    save_metadata.save(bucket, connection, imageids, all_bbox_descriptor, output_foldername)
    
    
    
