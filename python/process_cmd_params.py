# Process the command line arguments as user_email, user_param and user_selection, exception handling also happens here

import sys
import csv

def process():
    # read the labels file, to be used to convert user input labels to strings that other part of the application can recognize
    label_dict={}    
    with open("./data/labels.csv") as f:
        csv_reader=csv.reader(f, delimiter=',')
        for row in csv_reader:
            label_dict[row[1]]=row[0]
    # process command line arguments        
    user_email=sys.argv[1]
    user_param=[]
    # append user supplied image transform parameters and handle exceptions
    try:
        user_param.append(int(sys.argv[2]))
    except:
        user_param.append(256)
    try:
        user_param.append(int(sys.argv[3]))
        user_param.append(float(sys.argv[4]))
    except:
        user_param.append(1)
        user_param.append(0.0)
    try:
        user_param.append(float(sys.argv[5]))     
    except:
        user_param.append(1.0)
    try:
        user_param.append(float(sys.argv[6]))
        user_param.append(float(sys.argv[7]))     
        user_param.append(float(sys.argv[8]))     
        user_param.append(float(sys.argv[9]))
    except:
        user_param.append(0.0)
        user_param.append(1.0)   
        user_param.append(0.0)
        user_param.append(1.0)  
    
    if user_param[1]%2==0: # gaussian blur apperture size must be odd integer
        user_param[1]=user_param[1]+1
    if user_param[3]<1: # scale factor must be greater than or equal to 1.0
        user_param[3]=1
    if user_param[4]<0 or user_param[4]>=1: # left crop point must be between 0 and 1
        user_param[4]=0
    if user_param[5]<0 or user_param[5]>1: # right crop point must be between 0 and 1
        user_param[5]=1
    if user_param[5]<=user_param[4]: # right crop point must be greater than left crop point
        user_param[4]=0
        user_param[5]=1
    if user_param[6]<0 or user_param[6]>=1: # top crop point must be between 0 and 1
        user_param[6]=0
    if user_param[7]<0 or user_param[7]>1: # bottom crop point must be between 0 and 1
        user_param[7]=1
    if user_param[7]<=user_param[6]: # bottom crop point must be greater than top crop point
        user_param[6]=0
        user_param[7]=1
    
    # append user supplied image selection parameters and handle exceptions
    user_selection=[]
    try:
        user_selection.append(int(sys.argv[10]))
        user_selection.append(int(sys.argv[11]))
    except:
        user_selection.append(0)
        user_selection.append(99)
    try:
        user_selection.append(int(sys.argv[12]))
    except:
        user_selection.append(0)
    # convert the input labels to strings that other part of the application recognizes
    user_labels=[]
    for i in range(13, len(sys.argv)):
        key=sys.argv[i]
        if key in label_dict:
            user_selection.append(label_dict[key])
            user_labels.append(key)
    return [user_email, user_param, user_selection, user_labels]
