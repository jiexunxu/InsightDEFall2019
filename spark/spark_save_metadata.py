# Batch spark job for saving bounding box metadata

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import concat, col, lit

def save(output_foldername, query, db_password,  user_param):
    # Create 5 copies of the dataframe because we need to save the metadata for 5 types of transforms
    sc_conf=SparkConf().setAppName("SaveBBoxMetadataSession")
    sc_conf.setMaster("spark://ip-10-0-0-5:7077")
    sc_conf.set("spark.executor.memory", "4g")
    sc_conf.set("spark.executor.cores", 1)
    sc_conf.set("spark.executor.instances", 8)
    dbspark = SparkSession.builder.config(conf=sc_conf).getOrCreate()
    bbox_df_flip_horizontal=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", query).option("user", "postgres").option("password", db_password).load()
    bbox_df_flip_vertical=bbox_df_flip_horizontal.select("*")
    bbox_df_rotate=bbox_df_flip_horizontal.select("*")
    bbox_df_scale=bbox_df_flip_horizontal.select("*")
    bbox_df_crop=bbox_df_flip_horizontal.select("*")
    
    output_s3_name="s3a://jiexunxu-open-image-dataset/output_data/"+output_foldername+"selected-train-annotations-bbox-csv-parts"
    # user provided scaling parameter, greater than or equal to 1.0
    sc=user_param[3] 
    # user provided crop parameters, between 0 and 1
    cx1=user_param[4]
    cx2=user_param[5]
    cy1=user_param[6]
    cy2=user_param[7]
    # calculate and save the bounding box information for each of the transforms
    bbox_df_flip_horizontal.withColumn("x_min", 1-bbox_df_flip_horizontal.x_min).withColumn("x_max", 1-bbox_df_flip_horizontal.x_max).withColumn("imageid", concat(lit("fhb_"), bbox_df_flip_horizontal.imageid)).write.csv(output_s3_name+"-flip-horizontal")
    bbox_df_flip_vertical.withColumn("y_min", 1-bbox_df_flip_vertical.y_min).withColumn("y_max", 1-bbox_df_flip_vertical.y_max).withColumn("imageid", concat(lit("fvb_"), bbox_df_flip_vertical.imageid)).write.csv(output_s3_name+"-flip_vertical")
    bbox_df_rotate.withColumn("x_min", 1-bbox_df_rotate.x_min).withColumn("x_max", 1-bbox_df_rotate.x_max).withColumn("y_min", 1-bbox_df_rotate.y_min).withColumn("y_max", 1-bbox_df_rotate.y_max).withColumn("imageid", concat(lit("rb_"), bbox_df_rotate.imageid)).write.csv(output_s3_name+"-rotate")
    bbox_df_scale.withColumn("x_min", (bbox_df_scale.x_min-(sc-1)/2)/(2-sc)).withColumn("x_max", (bbox_df_scale.x_max-(sc-1)/2)/(2-sc)).withColumn("y_min", (bbox_df_scale.y_min-(sc-1)/2)/(2-sc)).withColumn("y_max", (bbox_df_scale.y_max-(sc-1)/2)/(2-sc)).withColumn("imageid", concat(lit("sb_"), bbox_df_scale.imageid)).write.csv(output_s3_name+"-scale")
    bbox_df_crop.withColumn("x_min", (bbox_df_crop.x_min-cx1)/(cx2-cx1)).withColumn("x_max", (bbox_df_crop.x_max-cx1)/(cx2-cx1)).withColumn("y_min", (bbox_df_crop.y_min-cy1)/(cy2-cy1)).withColumn("y_max", (bbox_df_crop.y_max-cy1)/(cy2-cy1)).withColumn("imageid", concat(lit("cb_"), bbox_df_crop.imageid)).write.csv(output_s3_name+"-crop")
