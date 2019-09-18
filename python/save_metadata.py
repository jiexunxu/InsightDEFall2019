# Saves the changed metadata to S3 in the same folder

def save(bucket, connection, imageids, all_bbox_descriptors, output_foldername):
    output_obj=bucket.Object(output_foldername+'selected-train-annotations-human-imagelabels-boxable.csv')
