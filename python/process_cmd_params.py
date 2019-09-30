# Process the command line arguments as user_email, user_param and user_selection, exception handling also happens here

import sys
import csv

def process():
    # read the labels file, to be used to convert user input labels to strings that other part of the application can recognize
    label_dict={}    
    with open("../data/labels.csv") as f:
        reader=csv.reader(f, delimiter=',')
        for row in csv_reader:
            label_dict[row[1]]=row[0]
    # process command line arguments        
    user_email=sys.argv[1]
    user_param=[int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5]), float(sys.argv[6]), float(sys.argv[7]), float(sys.argv[8]), float(sys.argv[9])]
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
    user_selection=[int(sys.argv[10]), int(sys.argv[11]), int(sys.argv[12])]
    # convert the input labels to strings that other part of the application recognizes
    for i in range(13, len(sys.argv)):
        key=sys.argv[i]
        if key in label_dict:
            user_selection.append(label_dict[key])
    return [user_email, user_param, user_selection]
