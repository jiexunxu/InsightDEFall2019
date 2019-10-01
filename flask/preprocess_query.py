# Preprocesses the user queries and return the command line arguments to be used in spark-submit

import sys
sys.path.insert(0, './python')
import process_cmd_params
import init
import select_images

def preprocess(request):
    email=request.form['email']
    obj_count=request.form['obj_count']
    labels=request.form['labels']
    image_size=request.form['image_size']
    scale=request.form['scale']
    crop=request.form['crop']
    blur=request.form['blur']
    google_only='1'
    
    try:
        tokens=obj_count.split(',')
        obj_count=[tokens[0] tokens[1]]
    except:
        obj_count=['0' '99']
    try:
        tokens=labels.split(',')
        labels=[]
        for token in tokens:
            labels.append(token)
    except:
        labels=['Apple']
    try:
        tokens=crop.split(',')
        crop=[tokens[0] tokens[1] tokens[2] tokens[3]]
    except:
        crop=['0.0' '1.0' '0.0' '1.0']
    try:
        tokens=blur.split(',')
        blur=[tokens[0] tokens[1]]
    except:
        blur=[1 0.0]
    
    arguments="./spark/spark_main_job.py  "+email+" "+image_size+" "+blur[0]+" "+blur[1]+" "+scale+" "+crop[0]+" "+crop[1]+" "+crop[2]+" "+crop[3]+" "+obj_count[0]+" "+obj_count[1]+" "+google_only+" "+labels    
    command="spark-submit --driver-class-path ~/postgresql-42.2.8.jar --jars ~/postgresql-42.2.8.jar --packages org.apache.spark:spark-avro_2.11:2.4.0,com.microsoft.ml.spark:mmlspark_2.11:0.18.1 "+arguments
    return command
