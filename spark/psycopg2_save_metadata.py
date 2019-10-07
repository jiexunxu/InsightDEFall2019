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
    bbox_scale=np.zeros((num_of_rows, 4))
    bbox_scale[:, 0]=[row[4] for row in bbox_descriptors]
    bbox_scale[:, 1]=[row[5] for row in bbox_descriptors]
    bbox_scale[:, 2]=[row[6] for row in bbox_descriptors]
    bbox_scale[:, 3]=[row[7] for row in bbox_descriptors]
    bbox_crop=np.copy(bbox_flip_horizontal)
    # user provided scaling parameter, greater than or equal to 1.0
    sc=user_param[3]
    # user provided crop parameters, between 0 and 1
    cx1=user_param[4]
    cx2=user_param[5]
    cy1=user_param[6]
    cy2=user_param[7]
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
        f.write("ImageID,Source,LabelName,Confidence,XMin,XMax,YMin,YMax,IsOccluded,IsTruncated,IsGroupOf,IsDepiction,IsInside, fhb_x_min, fhb_x_max, fhb_y_min, fhb_y_max, fvb_x_min, fvb_x_max, fvb_y_min, fvb_y_max, rb_x_min, rb_x_max, rb_y_min, rb_y_max, sb_x_min, sb_x_max, sb_y_min, sb_y_max, cb_x_min, cb_x_max, cb_y_min, cb_y_max\n")
        for i in range(len(bbox_descriptors)):
            row=bbox_descriptors[i]
            f.write(row[0]+","+row[1]+","+row[2]+","+row[3]+","+str(row[4])+","+str(row[5])+","+str(row[6])+","+str(row[7])+","+row[8]+","+row[9]+","+row[10]+","+row[11]+","+row[12]+","+str(row[13])+","+str(row[14])+","+str(row[15])+","+str(row[16])+","+str(row[17])+","+str(row[18])+","+str(row[19])+","+str(row[20])+","+str(row[21])+","+str(row[22])+","+str(row[23])+","+str(row[24])+","+str(bbox_scale[i][0])+","+str(bbox_scale[i][1])+","+str(bbox_scale[i][2])+","+str(bbox_scale[i][3])+","+str(bbox_crop[i][0])+","+str(bbox_crop[i][1])+","+str(bbox_crop[i][2])+","+str(bbox_crop[i][3])+"\n")
