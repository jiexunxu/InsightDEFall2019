import get_bbox_descriptors
from PIL import Image
from io import BytesIO

def process(bucket, connection, imageid, user_param, output_foldername):
    # Get the bbox descriptors associated with this image
    cursor=connection.cursor()
    query="SELECT * FROM image_bbox WHERE imageid='{}'".format(imageid)
    cursor.execute(query)
    bbox_descriptor=cursor.fetchall()
    # Enhance and save every image to the folder output_data in s3, and append the xmin, xmax, ymin and ymax of all enhanced descriptors to bbox_xy_enhanced
    bbox_xy_enhanced=[]
    for i in range(0, 8):
        img_enhanced=get_bbox_descriptors.get(bbox_descriptor, bbox_xy_enhanced, i, user_param[i])
    return [bbox_descriptor, bbox_xy_enhanced]
