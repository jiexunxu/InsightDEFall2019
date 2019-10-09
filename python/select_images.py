# This function accepts user input from the web UI and select the corresponding image names that match the user selection criteria

import psycopg2
import read_credentials

def select(connection, user_selection):
    # Selected images must contain boxable objects between min_obj_count and max_obj_count
    min_obj_count=user_selection[0]
    max_obj_count=user_selection[1]
    # Selected images must be google verified if is_google is 1
    is_google=user_selection[2]
    # Selected images can contain any of the user supplied labels
    target_labels=tuple(user_selection[3:len(user_selection)])
    cursor=connection.cursor()
    if is_google==1:
        cursor.execute('''SELECT imageid FROM image_selection WHERE obj_count>=%s AND obj_count<=%s AND is_google='1' AND label IN %s''', (min_obj_count, max_obj_count, target_labels,))
    else:
        cursor.execute('''SELECT imageid FROM image_selection WHERE obj_count>=%s AND obj_count<=%s AND label IN %s''', (min_obj_count, max_obj_count, target_labels,))
    imageids=cursor.fetchall()    
    return imageids
    
        
