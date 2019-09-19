# enhance a single image by its name

import transform_image
from PIL import Image
from io import BytesIO

def enhance(bucket, connection, imageid, user_param, output_foldername):
    # Read image from s3
    image_obj=bucket.Object('train_data/'+imageid+'.jpg')
    response=image_obj.get()
    img=Image.open(response['Body'])
    # Get the bbox descriptors associated with this image
    cursor=connection.cursor()
    query="SELECT * FROM image_bbox WHERE imageid='{}'".format(imageid)
    cursor.execute(query)
    bbox_descriptor=cursor.fetchall()
    # Enhance and save every image to the folder output_data in s3, and append the xmin, xmax, ymin and ymax of all enhanced descriptors to bbox_xy_enhanced
    bbox_xy_enhanced=[]
    for i in range(0, 8):
        img_enhanced=transform_image.transform(img, bbox_descriptor, bbox_xy_enhanced, i, user_param[i])
        # save image to corresponding s3 bucket
        output_obj=bucket.Object(output_foldername+imageid+"_{}.jpg".format(i))
        file_stream=BytesIO()
        img_enhanced.save(file_stream, format='jpeg')
        output_obj.put(Body=file_stream.getvalue())
    return [bbox_descriptor, bbox_xy_enhanced]
