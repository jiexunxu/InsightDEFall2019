# Batch transform images with spark

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from mmlspark.opencv import toNDArray
from mmlspark.opencv import ImageTransformer
from mmlspark.io import *

def transform(internal_params, s3_image_files, user_param):
    # Load all images from s3
    sc_conf=SparkConf().setAppName("BatchImageProcessing")
    if(len(s3_image_files)>internal_params[0]):
        sc_conf.setMaster("spark://ip-10-0-0-5:7077")
        sc_conf.set("spark.executor.memory", "4g")
        sc_conf.set("spark.executor.cores", 2)
        sc_conf.set("spark.num.executors", 4)
    spark = pyspark.sql.SparkSession.builder.config(conf=sc_conf).getOrCreate()
    images_df=spark.read.format("image").load(s3_image_files)
    # user provided resized image dimension
    L=user_param[0]
    # user provided gaussian blur parameters
    window=user_param[1] 
    sigma=user_param[2] 
    # user provided scaling parameter, greater than or equal to 1.0
    sc=user_param[3] 
    # user provided crop parameters, between 0 and 1
    cx1=user_param[4]
    cx2=user_param[5]
    cy1=user_param[6]
    cy2=user_param[7]
    # Resize image to L, flip horizontally, and apply gaussian blur
    flip_horizontal_blur_trans=(ImageTransformer().setOutputCol("fhb_").resize(L, L).flip(flipCode=1).gaussianKernel(window, sigma))
    # Resize image to L, flip vertically, and apply gaussian blur
    flip_vertical_blur_trans=(ImageTransformer().setOutputCol("fvb_").resize(L, L).flip(flipCode=0).gaussianKernel(window, sigma))
    # Resize image to L, rotate 180 degrees, and apply gaussian blur
    rotate_blur_trans=(ImageTransformer().setOutputCol("rb_").resize(L, L).flip(flipCode=0).flip(flipCode=1).gaussianKernel(window, sigma))
    # Resize image to L*sc, crop back to size L, and apply gaussian blur
    scale_blur_trans=(ImageTransformer().setOutputCol("sb_").resize(int(L*sc), int(L*sc)).crop(int(L*(sc-1)/2), int(L*(sc-1)/2), L, L).gaussianKernel(window, sigma))
    # Crop the image according to user specified crop parmaters, and apply gaussian blur
    crop_blur_trans=(ImageTransformer().setOutputCol("cb_").resize(L, L).crop(int(L*cx1), int(L*cy1), int(L*(cy2-cy1)), int(L*(cx2-cx1))).resize(L, L).gaussianKernel(window, sigma))
    # Apply the transforms
    images_df=flip_horizontal_blur_trans.transform(images_df)
    images_df=flip_vertical_blur_trans.transform(images_df)
    images_df=rotate_blur_trans.transform(images_df)
    images_df=scale_blur_trans.transform(images_df)
    images_df=crop_blur_trans.transform(images_df)
    return images_df
