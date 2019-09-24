# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 17:57:02 2019

Performs various type of image transforms to augment data for image based machine learning
Parameters:
    img is the source image, read with python's PIL library, it is colored
    bbox_descriptor is a list of list of descriptors associated with img in train-annotations-bbox.csv, it is a list of the following entries: 
    ImageID,Source,LabelName,Confidence,XMin,XMax,YMin,YMax,IsOccluded,IsTruncated,IsGroupOf,IsDepiction,IsInside
    for transform_type 0~5 the XMin, XMax, YMin and YMax values needs to be changed
    transform_type indicates different transform types, specifically:
        1: flip horizontally if param==0, flip vertically otherwise
        2: rotate 90 degrees counter-clockwise if param==0, rotate 180 degrees counter-clockwise if param==1, rotate 270 degrees countr-clockwise otherwise
        3: scale image up by param then crop the image to its original size
        4: tweak the brightness of the image by param
        5: tweak the contrast of the image by param
        6: tweak the sharpness of the image by param
        7: gaussian blur the image by radius param
@author: Jiexun Xu
"""

from PIL import Image, ImageEnhance, ImageFilter

def get(bbox_descriptor, bbox_descriptor_enhanced, transform_type, param):
    width=img.width
    height=img.height
    bbox_count=len(bbox_descriptor)
    bbox_enhanced_count=len(bbox_descriptor_enhanced)
    for i in range(bbox_count):
        bbox_descriptor_enhanced.append([bbox_descriptor[i][4], bbox_descriptor[i][5], bbox_descriptor[i][6], bbox_descriptor[i][7]])
    if transform_type==1:
        if param==0:
            for i in range(bbox_count):
                bbox_descriptor_enhanced[bbox_enhanced_count+i][0]=1-bbox_descriptor[i][5]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][1]=1-bbox_descriptor[i][4]
        else:
            for i in range(bbox_count):
                bbox_descriptor_enhanced[bbox_enhanced_count+i][2]=1-bbox_descriptor[i][7]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][3]=1-bbox_descriptor[i][6]
    elif transform_type==2:
        if param==0:
            for i in range(bbox_count):
                bbox_descriptor_enhanced[bbox_enhanced_count+i][0]=bbox_descriptor[i][6]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][1]=bbox_descriptor[i][7]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][2]=1-bbox_descriptor[i][5]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][3]=1-bbox_descriptor[i][4]
        elif param==1:
            for i in range(bbox_count):
                bbox_descriptor_enhanced[bbox_enhanced_count+i][0]=1-bbox_descriptor[i][5]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][1]=1-bbox_descriptor[i][4]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][2]=1-bbox_descriptor[i][7]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][3]=1-bbox_descriptor[i][6]
        else:
            for i in range(bbox_count):
                bbox_descriptor_enhanced[bbox_enhanced_count+i][0]=1-bbox_descriptor[i][7]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][1]=1-bbox_descriptor[i][6]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][2]=bbox_descriptor[i][4]
                bbox_descriptor_enhanced[bbox_enhanced_count+i][3]=bbox_descriptor[i][5]
    elif transform_type==3:        
        for i in range(bbox_count):
            bbox_descriptor_enhanced[bbox_enhanced_count+i][0]=bbox_descriptor[i][4]-param/2
            bbox_descriptor_enhanced[bbox_enhanced_count+i][1]=bbox_descriptor[i][5]+param/2
            bbox_descriptor_enhanced[bbox_enhanced_count+i][2]=bbox_descriptor[i][6]-param/2
            bbox_descriptor_enhanced[bbox_enhanced_count+i][3]=bbox_descriptor[i][7]+param/2
