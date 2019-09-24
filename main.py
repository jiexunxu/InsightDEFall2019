# The main pipeline

import sys
sys.path.insert(0, './python')
sys.path.insert(0, './spark')
sys.path.insert(0, './postgresql')
sys.path.insert(0, './flask')
import init
import select_images
import enhance_single_image
import save_metadata
import notify_user

def start():
    user_selection=[17, 18, 1, '/m/01_5g']
    user_param=[0, 0, 1, 0.3, 0.5, 1.8, 2.0, 3]
    user_email='gexelenor@4nextmail.com'
    [bucket, connection, output_foldername]=init.init()
    imageids=select_images.select(connection, user_selection[0], user_selection[1], user_selection[2], user_selection[3])
    # later in spark we will divide imageids into 4 parts and assign each part to a EC2 instance to do the job below
    all_bbox_descriptor=[]
    local_file_name1="tmp1.tmpdump~"
    with open(local_file_name1, 'w') as f:
        f.write("ImageID,Source,LabelName,Confidence\n")
    local_file1=open(local_file_name1, 'a')
    local_file_name2="tmp2.tmpdump~"
    with open(local_file_name2, 'w') as f:
        f.write("ImageID,Source,LabelName,Confidence,XMin,XMax,YMin,YMax,IsOccluded,IsTruncated,IsGroupOf,IsDepiction,IsInside\n")
    local_file2=open(local_file_name2, 'a')
    print('selected image count:'+str(len(imageids)))
    image_count=0
    for imageid in imageids:
        [bbox_descriptor, bbox_xy_enhanced]=enhance_single_image.enhance(bucket, connection, imageid[0], user_param, output_foldername)
        all_bbox_descriptor.append(bbox_descriptor)
        save_metadata.save(bucket, connection, imageid[0], bbox_descriptor, bbox_xy_enhanced, output_foldername, local_file1, local_file2)
        image_count+=1
        print('enhanced {} images'.format(image_count))
    local_file1.close()
    local_file2.close()
    with open(local_file_name1, 'rb') as body:
        bucket.put_object(Key=output_foldername+'selected-train-annotations-human-imagelabels-boxable.csv', Body=body)
    with open(local_file_name2, 'rb') as body:
        bucket.put_object(Key=output_foldername+'selected-train-annotations-bbox.csv', Body=body)
    notify_user.email_and_log(user_email, user_param)

    
    
    
