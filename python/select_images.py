# This function accepts user input from the web UI, select the corresponding images that match the user selection criteria

# Parameters:
# max_obj_count: Maximum number of boxable objects an image can have. Selected images must have this number of objects or less
#is_google: If it is 1, can only select images from google verified; otherwise all images are eligible
# target_label: images must contain target_label

import psycopg2
import read_credentials

def select(connection, max_obj_count, is_google, target_label):
    cursor=connection.cursor()
    if is_google==1:
        cursor.execute('SELECT imageid FROM image_selection WHERE obj_count<=%d, is_google=1, label=%s', (max_obj_count, target_label))
    else:
        cursor.execute('SELECT imageid FROM image_selection WHERE obj_count<=%d, label=%s', (max_obj_count, target_label))
    imageids=cursor.fetchall()    
    return imageids
    
        
