# Preprocesses the user queries and return the command line arguments to be used in spark-submit

import sys
sys.path.insert(0, './python')
import process_cmd_params
import init
import select_images

def preprocess(request):
    email=request.form['email']
    min_obj=request.form['min_obj']
    max_obj=request.form['max_obj']
    box_source=request.form['box_source']
    labels=request.form['labels']
    image_size=request.form['image_size']
    scale=request.form['scale']
    crop=request.form['crop']
    blur_size=request.form['blur_size']
    blur_sigma=request.form['blur_sigma']
    
    try:
        if box_source == 'Google':
            box_source='1'
        else:
            box_source='0'
    except:
        box_source='0'
    try:
        tokens=labels.split(',')
        labels=[]
        for token in tokens:
            labels.append(token)
    except:
        labels=['Apple']
    try:
        tokens=crop.split(',')
        crop=[tokens[0], tokens[1], tokens[2], tokens[3]]
    except:
        crop=['0.0', '1.0', '0.0', '1.0']
    
    arguments="./spark/spark_main_job.py  "+email+" "+image_size+" "+blur_size+" "+blur_sigma+" "+scale+" "+crop[0]+" "+crop[1]+" "+crop[2]+" "+crop[3]+" "+min_obj+" "+max_obj+" "+box_source
    for label in labels:
        arguments+=" "+label 
    command="spark-submit --driver-class-path /home/ubuntu/postgresql-42.2.8.jar --jars /home/ubuntu/postgresql-42.2.8.jar --packages org.apache.spark:spark-avro_2.11:2.4.0,com.microsoft.ml.spark:mmlspark_2.11:0.18.1 "+arguments
    return command
