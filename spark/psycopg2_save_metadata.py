# Use psycopg2 to access database, calcluate bounding boxes and save metadata

import numpy as np
import s3fs

def save(connection, query, user_param, output_foldername):
    cursor=connection.cursor()
    cursor.execute(query)
    # Get all bouding box entries that match the imageids in the query
    bbox_descriptors=cursor.fetchall()
    num_of_rows=len(bbox_descriptors)
    # copy the bounding boxes numbers into numpy arrays for faster computation
    bbox_flip_horizontal=np.zeros((num_of_rows, 4))
    bbox_flip_horizontal[:, 0]=[row[4] for row in bbox_descriptors]
    bbox_flip_horizontal[:, 1]=[row[5] for row in bbox_descriptors]
    bbox_flip_horizontal[:, 2]=[row[6] for row in bbox_descriptors]
    bbox_flip_horizontal[:, 3]=[row[7] for row in bbox_descriptors]
    bbox_flip_vertical=np.copy(bbox_flip_horizontal)
    bbox_rotate=np.copy(bbox_flip_horizontal)
    bbox_scale=np.copy(bbox_flip_horizontal)
    bbox_crop=np.copy(bbox_flip_horizontal)
    # user provided scaling parameter, greater than or equal to 1.0
    sc=user_param[3]
    # user provided crop parameters, between 0 and 1
    cx1=user_param[4]
    cx2=user_param[5]
    cy1=user_param[6]
    cy2=user_param[7]
    # compute bounding boxes for vertical flip
    bbox_flip_horizontal[:, 0]=1-bbox_flip_horizontal[:, 0]
    bbox_flip_horizontal[:, 1]=1-bbox_flip_horizontal[:, 1]
    # compute bounding boxes for horizontal flip
    bbox_flip_vertical[:, 2]=1-bbox_flip_vertical[:, 2]
    bbox_flip_vertical[:, 3]=1-bbox_flip_vertical[:, 3]
    # compute bounding boxes for 180 degree rotation
    bbox_rotate[:, 0]=1-bbox_rotate[:, 0]
    bbox_rotate[:, 1]=1-bbox_rotate[:, 1]
    bbox_rotate[:, 2]=1-bbox_rotate[:, 2]
    bbox_rotate[:, 3]=1-bbox_rotate[:, 3]
    # compute bounding boxes for scaling images and then crop to the fixed size
    bbox_scale[:, 0]=bbox_scale[:, 0]-(sc-1)/2
    bbox_scale[:, 1]=bbox_scale[:, 1]-(sc-1)/2
    bbox_scale[:, 2]=bbox_scale[:, 2]-(sc-1)/2
    bbox_scale[:, 3]=bbox_scale[:, 3]-(sc-1)/2
    # compute bounding boxes for croping the image into user defined sizes
    bbox_crop[:, 0]=(bbox_crop[:, 0]-cx1)/(cx2-cx1)
    bbox_crop[:, 1]=(bbox_crop[:, 1]-cx1)/(cx2-cx1)
    bbox_crop[:, 2]=(bbox_crop[:, 2]-cy1)/(cy2-cy1)
    bbox_crop[:, 3]=(bbox_crop[:, 3]-cy1)/(cy2-cy1)
    # save the updated metadata to s3
    s3=s3fs.S3FileSystem(anon=False)
    with s3.open("jiexunxu-open-image-dataset/output_data/"+output_foldername+"selected-train-annotations-bbox.csv", "w") as f:
        f.write("ImageID,Source,LabelName,Confidence,XMin,XMax,YMin,YMax,IsOccluded,IsTruncated,IsGroupOf,IsDepiction,IsInside\n")
        for i in range(len(bbox_descriptors)):
            row=bbox_descriptors[i]
            f.write("fhb_"+row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(bbox_flip_horizontal[i][0])+","+str(bbox_flip_horizontal[i][1])+","+str(bbox_flip_horizontal[i][2])+","+str(bbox_flip_horizontal[i][3])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+"\n")
            f.write("fvb_"+row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(bbox_flip_vertical[i][0])+","+str(bbox_flip_vertical[i][1])+","+str(bbox_flip_vertical[i][2])+","+str(bbox_flip_vertical[i][3])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+"\n")
            f.write("rb_"+row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(bbox_rotate[i][0])+","+str(bbox_rotate[i][1])+","+str(bbox_rotate[i][2])+","+str(bbox_rotate[i][3])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+"\n")
            f.write("sb_"+row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(bbox_scale[i][0])+","+str(bbox_scale[i][1])+","+str(bbox_scale[i][2])+","+str(bbox_scale[i][3])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+"\n")
            f.write("cb_"+row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(bbox_crop[i][0])+","+str(bbox_crop[i][1])+","+str(bbox_crop[i][2])+","+str(bbox_crop[i][3])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+"\n")        
