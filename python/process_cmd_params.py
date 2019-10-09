# Process the command line arguments as user_email, user_param and user_selection, exception handling also happens here

import sys
import csv
import argparse

def process():
    # read the labels file, to be used to convert user input labels to strings that other part of the application can recognize
    label_dict={}    
    with open("./data/labels.csv") as f:
        csv_reader=csv.reader(f, delimiter=',')
        for row in csv_reader:
            label_dict[row[1]]=row[0]
    # process command line arguments with the argparse module        
    parser=argparse.ArgumentParser(description='spark-submit arguments')
    parser.add_argument("user_email", default="testmail@mailnow2.com", type=str)
    parser.add_argument("resize_dim", default=256, type=int)
    parser.add_argument("gaussian_aperture", default=1, type=int)
    parser.add_argument("gaussian_sigma", default=0.0, type=float)
    parser.add_argument("scale_factor", default=1.0, type=float)
    parser.add_argument("crop_x_min", default=0.0, type=float)
    parser.add_argument("crop_x_max", default=1.0, type=float)
    parser.add_argument("crop_y_min", default=0.0, type=float)
    parser.add_argument("crop_y_max", default=1.0, type=float)
    parser.add_argument("min_objects", default=12, type=int)
    parser.add_argument("max_objects", default=14, type=int)
    parser.add_argument("verification_source", default=1, type=int)
    parser.add_argument("image_labels", default="Snowman", type=str, nargs=argparse.REMAINDER)
    args=parser.parse_args()
    user_email=args.user_email
    user_param=[args.resize_dim, args.gaussian_aperture, args.gaussian_sigma, args.scale_factor, args.crop_x_min, args.crop_x_max, args.crop_y_min, args.crop_y_max]
    user_selection=[args.min_objects, args.max_objects, args.verification_source]
    # convert the input labels to strings that other part of the application recognizes
    input_labels=args.image_labels
    user_labels=[]
    for key in input_labels:
        if key in label_dict:
            user_selection.append(label_dict[key])
            user_labels.append(key)
    return [user_email, user_param, user_selection, user_labels]
