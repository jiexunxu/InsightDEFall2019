# Batch spark job for saving bounding box metadata

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import concat, col, lit
import subprocess

def save(output_foldername, query, db_password,  user_param):
    # Create 5 copies of the dataframe because we need to save the metadata for 5 types of transforms
    sc_conf=SparkConf().setAppName("SaveBBoxMetadataSession")
    sc_conf.setMaster("spark://ip-10-0-0-5:7077")
    sc_conf.set("spark.executor.memory", "4g")
    sc_conf.set("spark.executor.cores", 2)
    sc_conf.set("spark.executor.instances", 4)
    dbspark = SparkSession.builder.config(conf=sc_conf).getOrCreate()
    bbox_df=dbspark.read.format("jdbc").option("url",  "jdbc:postgresql://ec2-3-230-4-222.compute-1.amazonaws.com/imagedb").option("query", query).option("user", "postgres").option("password", db_password).load()
    
    output_s3_name="s3a://jiexunxu-open-image-dataset/output_data/"+output_foldername+"selected-train-annotations-bbox-csv-parts"
    # user provided scaling parameter, greater than or equal to 1.0
    sc=user_param[3] 
    # user provided crop parameters, between 0 and 1
    cx1=user_param[4]
    cx2=user_param[5]
    cy1=user_param[6]
    cy2=user_param[7]
   # for i in range(1, 6):
   #     command="spark-submit --driver-class-path /home/ubuntu/postgresql-42.2.8.jar --jars /home/ubuntu/postgresql-42.2.8.jar ./spark/spark_save_metadata_single_transform.py "+str(i)+" "+db_password+"  "+str(sc)+" "+str(cx1)+" "+str(cx2)+" "+str(cy1)+" "+str(cy2)+" "+output_foldername+" "+query
   #     subprocess.Popen(command.split())
    # calculate and save the bounding box information for each of the transforms
    bbox_df=bbox_df.withColumn("sb_x_min", (bbox_df.x_min-(sc-1)/2)/(2-sc)).withColumn("sb_x_max", (bbox_df.x_max-(sc-1)/2)/(2-sc)).withColumn("sb_y_min", (bbox_df.y_min-(sc-1)/2)/(2-sc)).withColumn("sb_y_max", (bbox_df.y_max-(sc-1)/2)/(2-sc)).withColumn("cb_x_min", (bbox_df.x_min-cx1)/(cx2-cx1)).withColumn("cb_x_max", (bbox_df.x_max-cx1)/(cx2-cx1)).withColumn("cb_y_min", (bbox_df.y_min-cy1)/(cy2-cy1)).withColumn("cb_y_max", (bbox_df.y_max-cy1)/(cy2-cy1)).write.csv(output_s3_name+"-csv-parts", header="true")
 #   bbox_df.withColumn("sb_x_min", (bbox_df.x_min-(sc-1)/2)/(2-sc)).withColumn("sb_x_max", (bbox_df.x_max-(sc-1)/2)/(2-sc)).withColumn("sb_y_min", (bbox_df.y_min-(sc-1)/2)/(2-sc)).withColumn("sb_y_max", (bbox_df.y_max-(sc-1)/2)/(2-sc)).write.csv(output_s3_name+"-sb", header="true")
 #   bbox_df.withColumn("cb_x_min", (bbox_df.x_min-cx1)/(cx2-cx1)).withColumn("cb_x_max", (bbox_df.x_max-cx1)/(cx2-cx1)).withColumn("cb_y_min", (bbox_df.y_min-cy1)/(cy2-cy1)).withColumn("cb_y_max", (bbox_df.y_max-cy1)/(cy2-cy1)).write.csv(output_s3_name+"-cb", header="true")
