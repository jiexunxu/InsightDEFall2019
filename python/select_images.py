# This function accepts user input from the web UI, select the corresponding images that match the user selection criteria

# Parameters:
# min_obj_count and max_obj_count: Maximum number of boxable objects an image can have. Selected images must have number of objects between these two (inclusive)
#is_google: If it is 1, can only select images from google verified; otherwise all images are eligible
# target_label: images must contain target_label

import psycopg2
import read_credentials

def select(connection, user_selection):
    min_obj_count=user_selection[0]
    max_obj_count=user_selection[1]
    is_google=user_selection[2]
    target_labels=tuple(user_selection[3:len(user_selection)])
    cursor=connection.cursor()
    if is_google==1:
        cursor.execute('''SELECT imageid FROM image_selection WHERE obj_count>=%s AND obj_count<=%s AND is_google='1' AND label IN %s''', (min_obj_count, max_obj_count, target_labels,))
    else:
        cursor.execute('''SELECT imageid FROM image_selection WHERE obj_count>=%s AND obj_count<=%s AND label IN %s''', (min_obj_count, max_obj_count, target_labels,))
    imageids=cursor.fetchall()    
    return imageids
    
        
