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
    sc_conf.set("spark.executor.cores", 2)
    sc_conf.set("spark.executor.instances", 4)
    dbspark = SparkSession.builder.config(conf=sc_conf).getOrCreate()
    bbox_df=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", query).option("user", "postgres").option("password", db_password).load().cache()
    
    output_s3_name="s3a://jiexunxu-open-image-dataset/output_data/"+output_foldername+"selected-train-annotations-bbox-csv-parts"
    # user provided scaling parameter, greater than or equal to 1.0
    sc=user_param[3] 
    # user provided crop parameters, between 0 and 1
    cx1=user_param[4]
    cx2=user_param[5]
    cy1=user_param[6]
    cy2=user_param[7]
    # calculate and save the bounding box information for each of the transforms
    bbox_df.withColumn("fhb_x_min", 1-bbox_df.x_min).withColumn("fhb_x_max", 1-bbox_df.x_max).withColumn("fhb_y_min", bbox_df["y_min"]).withColumn("fhb_y_max", bbox_df["y_max"]).withColumn("fvb_x_min", bbox_df["x_min"]).withColumn("fvb_x_max", bbox_df["x_max"]).withColumn("fvb_y_min", 1-bbox_df.y_min).withColumn("fvb_y_max", 1-bbox_df.y_max).withColumn("rb_x_min", 1-bbox_df.x_min).withColumn("rb_x_max", 1-bbox_df.x_max).withColumn("rb_y_min", 1-bbox_df.y_min).withColumn("rb_y_max", 1-bbox_df.y_max).withColumn("sb_x_min", (bbox_df.x_min-(sc-1)/2)/(2-sc)).withColumn("sb_x_max", (bbox_df.x_max-(sc-1)/2)/(2-sc)).withColumn("sb_y_min", (bbox_df.y_min-(sc-1)/2)/(2-sc)).withColumn("sb_y_max", (bbox_df.y_max-(sc-1)/2)/(2-sc)).withColumn("cb_x_min", (bbox_df.x_min-cx1)/(cx2-cx1)).withColumn("cb_x_max", (bbox_df.x_max-cx1)/(cx2-cx1)).withColumn("cb_y_min", (bbox_df.y_min-cy1)/(cy2-cy1)).withColumn("cb_y_max", (bbox_df.y_max-cy1)/(cy2-cy1)).write.csv(output_s3_name+"-csv-parts", header="true")
