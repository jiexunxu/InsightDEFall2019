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
        0: flip horizontally if param==0, flip vertically if param==1
        1: rotate 90 degrees counter-clockwise if param==0, rotate 180 degrees counter-clockwise if param==1, rotate 270 degrees countr-clockwise if param==2
        2: scale image up by param then crop the image to its original size
        3: tweak the brightness of the image by param
        4: tweak the contrast of the image by param
        5: tweak the sharpness of the image by param
        6: blur the image
@author: Jiexun Xu
"""

from PIL import Image, ImageEnhance, ImageFilter

def transform(img, bbox_descriptor, transform_type, param):
    width=img.width
    height=img.height
    xmin=[]
    xmax=[]
    ymin=[]
    ymax=[]
    for i in range(len(bbox_descriptor)):
        xmin.append(bbox_descriptor[i][4])
        xmax.append(bbox_descriptor[i][5])
        ymin.append(bbox_descriptor[i][6])
        ymax.append(bbox_descriptor[i][7])
    if transform_type==0:
        if param==0:
            img=img.transpose(Image.FLIP_LEFT_RIGHT)
            for i in range(len(bbox_descriptor)):
                bbox_descriptor[i][4]=1-xmax[i]
                bbox_descriptor[i][5]=1-xmin[i]
        elif param==1:
            img=img.transpose(Image.FLIP_TOP_BOTTOM)
            for i in range(len(bbox_descriptor)):
                bbox_descriptor[i][6]=1-ymax[i]
                bbox_descriptor[i][7]=1-ymin[i]
    elif transform_type==1:
        if param==0:
            img=img.rotate(90)
            for i in range(len(bbox_descriptor)):
                bbox_descriptor[i][4]=ymin[i]
                bbox_descriptor[i][5]=ymax[i]
                bbox_descriptor[i][6]=1-xmax[i]
                bbox_descriptor[i][7]=1-xmin[i]
        elif param==1:
            img=img.rotate(180) 
            for i in range(len(bbox_descriptor)):
                bbox_descriptor[i][4]=1-xmax[i]
                bbox_descriptor[i][5]=1-xmin[i]
                bbox_descriptor[i][6]=1-ymax[i]
                bbox_descriptor[i][7]=1-ymin[i]
        elif param==2:
            img=img.rotate(270)
            for i in range(len(bbox_descriptor)):
                bbox_descriptor[i][4]=1-ymax[i]
                bbox_descriptor[i][5]=1-ymin[i]
                bbox_descriptor[i][6]=xmin[i]
                bbox_descriptor[i][7]=xmax[i]
    elif transform_type==2:        
        img=img.resize((int(width*(1+param)), int(height*(1+param))), Image.BICUBIC).crop(
        (int(width*param/2), int(height*param/2), int(width*(1+param/2)), int(height*(1+param/2))))
        for i in range(len(bbox_descriptor)):
            bbox_descriptor[i][4]=xmin[i]-param/2
            bbox_descriptor[i][5]=xmax[i]+param/2
            bbox_descriptor[i][6]=ymin[i]-param/2
            bbox_descriptor[i][7]=ymax[i]+param/2
    elif transform_type==3:
        img=ImageEnhance.Brightness(img).enhance(param)
    elif transform_type==4:
        img=ImageEnhance.Contrast(img).enhance(param)
    elif transform_type==5:
        img=ImageEnhance.Sharpness(img).enhance(param)
    elif transform_type==6:
        img=img.filter(ImageFilter.GaussianBlur)
    
    return [img, bbox_descriptor]
