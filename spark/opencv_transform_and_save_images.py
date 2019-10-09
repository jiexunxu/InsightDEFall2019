# This function reads individual images from s3, augments them with opencv locally, and save them in a specific folder
import cv2
import boto3
import numpy as np
from PIL import Image
import subprocess


def process(bucket, img_name, output_foldername, L, w, sigma, sc, cx1, cx2, cy1, cy2):
    # read image from s3
    image_obj = bucket.Object("train_data/" + img_name + ".jpg")
    response = image_obj.get()
    img = Image.open(response["Body"]).convert("RGB")
    img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
    # apply the 5 transforms to the image
    # transform 1: Resize to L by L, flip horizontally, and gaussian blur
    # transform 2: Resize to L by L, flip vertically, and gaussian blur
    # transform 3: Resize to L by L, rotate 180, and gaussian blur
    # transform 4: Resize to L*sc by L*sc, crop down to L by L, and gaussian blur
    # transfomr 5: Crop to the rectangle defined as W*cx1, W*cx2, H*cy1, H*cy2, resize to L by L, and gaussian blur
    img_resized = cv2.resize(img.copy(), (L, L), interpolation=cv2.INTER_AREA)
    fhb_img = cv2.GaussianBlur(cv2.flip(img_resized, 1), (w, w), sigma)
    fvb_img = cv2.GaussianBlur(cv2.flip(img_resized, 0), (w, w), sigma)
    rb_img = cv2.GaussianBlur(cv2.flip(img_resized, -1), (w, w), sigma)
    sb_img = cv2.resize(img.copy(), (int(L * sc), int(L * sc)), interpolation=cv2.INTER_AREA)
    start_xy = int((sc - 1) / 2 * L)
    sb_img = sb_img[start_xy : start_xy + L, start_xy : start_xy + L]
    sb_img = cv2.GaussianBlur(sb_img, (w, w), sigma)
    [height, width, nchannels] = img.shape
    cb_img = img[
        int(height *cy1) : int(height * cy2), int(width * cx1) : int(width * cx2)
    ]
    cb_img = cv2.resize(cb_img, (L, L), interpolation=cv2.INTER_AREA)
    cb_img = cv2.GaussianBlur(cb_img, (w, w), sigma)
    # Save the augmented images locally
    cv2.imwrite("./" + output_foldername + "fhb_" + img_name + ".jpg", fhb_img)
    cv2.imwrite("./" + output_foldername + "fvb_" + img_name + ".jpg", fvb_img)
    cv2.imwrite("./" + output_foldername + "rb_" + img_name + ".jpg", rb_img)
    cv2.imwrite("./" + output_foldername + "sb_" + img_name + ".jpg", sb_img)
    cv2.imwrite("./" + output_foldername + "cb_" + img_name + ".jpg", cb_img)
