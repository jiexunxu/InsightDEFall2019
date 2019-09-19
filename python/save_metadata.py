# Saves the changed metadata to S3 in the same folder

from io import StringIO

def save(bucket, connection, imageid, bbox_descriptor, bbox_xy_enhanced, output_foldername, local_file1, local_file2):
    cursor=connection.cursor()
    query="SELECT * FROM image_labels WHERE imageid='{}'".format(imageid)
    cursor.execute(query)
    image_label_entry_tuples=cursor.fetchall()
    for tuple in image_label_entry_tuples:
        for i in range(8):
            local_file1.write(imageid+"_"+str(i)+","+tuple[1]+","+tuple[2]+","+tuple[3]+"\n")    
    tuple_count=0
    for i in range(len(bbox_descriptor)):
        tuple=bbox_descriptor[i]
        for j in range(8):
            local_file2.write(imageid+"_"+str(i)+","+tuple[1]+","+tuple[2]+","+tuple[3]+","+str(bbox_xy_enhanced[i*8+j][0])+","+str(bbox_xy_enhanced[i*8+j][1])+","+str(bbox_xy_enhanced[i*8+j][2])+","+str(bbox_xy_enhanced[i*8+j][3])+","+tuple[8]+","+tuple[9]+","+tuple[10]+","+tuple[11]+","+tuple[12]+"\n")
        tuple_count+=1
    
