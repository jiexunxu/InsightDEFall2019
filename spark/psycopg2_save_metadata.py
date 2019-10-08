# Use psycopg2 to access database, calcluate bounding boxes and save metadata

import numpy as np
import s3fs

def save(connection, query, user_param, output_foldername):
    cursor=connection.cursor()
    cursor.execute(query)
    # Get all bouding box entries that match the imageids in the query
    bbox_descriptors=cursor.fetchall()
    # user provided scaling parameter, greater than or equal to 1.0
    sc=user_param[3]
    # user provided crop parameters, between 0 and 1
    cx1=user_param[4]
    cx2=user_param[5]
    cy1=user_param[6]
    cy2=user_param[7]
    # save the updated metadata to s3
    s3=s3fs.S3FileSystem(anon=False)
    with s3.open("jiexunxu-open-image-dataset/output_data/"+output_foldername+"selected-train-annotations-bbox.csv", "w") as f:
        f.write("ImageID,label,x_min,x_max,y_min,y_max,fhb_x_min,fhb_x_max,fhb_y_min,fhb_y_max,fvb_x_min,fvb_x_max,fvb_y_min,fvb_y_max,rb_x_min,rb_x_max,rb_y_min,rb_y_max,sb_x_min,sb_x_max,sb_y_min,sb_y_max,cb_x_min,cb_x_max,cb_y_min,cb_y_max\n")
        for row in bbox_descriptors:
            for i in range(len(row[1])):
                x_min=row[2][i]
                x_max=row[3][i]
                y_min=row[4][i]
                y_max=row[5][i]
                f.write(row[0]+","+row[1][i]+","+str(row[2][i])+","+str(row[3][i])+","+str(row[4][i])+","+str(row[5][i])+","+str(row[6][i])+","+str(row[7][i])+","+str(row[8][i])+","+str(row[9][i])+","+str(row[10][i])+","+str(row[11][i])+","+str(row[12][i])+","+str(row[13][i])+","+str(row[14][i])+","+str(row[15][i])+","+str(row[16][i])+","+str(row[17][i])+","+str((x_min-(sc-1)/2)/(2-sc))+","+str((x_max-(sc-1)/2)/(2-sc))+","+str((y_min-(sc-1)/2)/(2-sc))+","+str((y_max-(sc-1)/2)/(2-sc))+","+str((x_min-cx1)/(cx2-cx1))+","+str((x_max-cx1)/(cx2-cx1))+","+str((y_min-cy1)/(cy2-cy1))+","+str((y_max-cy1)/(cy2-cy1))+"\n")
