# enhance a single image by its name

import boto3
from PIL import Image
from io import BytesIO

def enhance(filename, bbox_descriptor, user_param):
    # Read image from s3
    s3=boto3.resource('s3')
    bucket=s3.Bucket('jiexunxu-open-image-dataset') 
    image_obj=bucket.Object('train_data/'+filename+'.jpg')
    response=image_obj.get()
    img=Image.open(response['Body'])
    # Enhance and save every image to the folder output_data in s3
    for i in range(0, 10):
        [img_enhanced, bbox_descriptor_enhanced]=transform_image(img, bbox_descriptor, i, user_param[i])
        output_obj=bucket.Object('output_data/'+filename+'_{}.jpg'.format(i))
        file_stream=BytesIO()
        img_enhanced.save(file_stream, format='jpeg')
        output_obj.put(Body=file_stream.getvalue())
