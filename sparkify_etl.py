# import libs
import configparser
import os
import glob
import pandas as pd
from datetime import *

# spark imports and configurations
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from udf import timestamp_udf

# Configrations to connect with AWS services
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']

SOURCE_S3_LOGS_BUCKET = config['SOURCE_S3_LOGS_BUCKET']
SOURCE_S3_SONGS_BUCKET = config['SOURCE_S3_SONGS_BUCKET']
DEST_S3_BUCKET = config['DEST_S3_BUCKET']


def process_songs(spark, InputPath):
    """
    Description: process_songs function
        - Function to import song files using InputPath, clean the data in song files,
          and Extract features to load it to the analytical tables files
        - ETL part of the songs files

    Parameters:
    spark: created spark session to use within the function
    InputPath: path \ URL of input files
    """
    # read song files
    song_df = spark.read.json(
        InputPath, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record')
    
    # clean
    song_df = song_df.na.drop()
    song_df = song_df.drop_duplicates()

    return song_df


def process_logs(spark, InputPath):
    """
    Description: process_logs function
        - Function to import log files using InputPath, clean the data in log files,
          and Extract features to load it to the analytical tables files
        - ETL part of the logs files

    Parameters:
    spark: created spark session to use within the function
    InputPath: path \ URL of input files
    """
    # read the logs 
    logs_df = spark.read.json(
        InputPath, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record')

    # filter the data
    logs_df = logs_df.filter(logs_df.page == "NextSong")

    # clean
    logs_df = logs_df.drop_duplicates()
    logs_df = logs_df.na.drop()

    # convert Time stamp and extract datetime features
    logs_df = logs_df.withColumn("timestamp", timestamp_udf(F.col("ts")))

    logs_df = logs_df.withColumn("year", F.year("datetime"))
    logs_df = logs_df.withColumn("month", F.month("datetime"))
    logs_df = logs_df.withColumn("weekofyear", F.weekofyear("datetime"))
    logs_df = logs_df.withColumn("day", F.dayofmonth("datetime"))
    logs_df = logs_df.withColumn("hour", F.hour("datetime"))
    logs_df = logs_df.withColumn("weekday", F.dayofweek("datetime"))

    return logs_df


def load_tables(spark, logs_df, song_df, OutputPath):
    """
    Description: load_tables function
        - Function to load logs_df. and song_df, and select features to load it to the analytical tables files

    Parameters:
    logs_df: cleansed logs_df
    song_df: cleansed song_df
    OutputPath: path \ URL to locations of saved analytical tables
    """

    # select features of each table
    song_table = song_df.select(
        "song_id", "title", "artist_id", "year", "duration")
    
    artist_table = song_df.select(
        "artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    users_table = logs_df.select(
        "userid", "firstName", "lastName", "gender", "level")
    
    time_table = logs_df.select(
        "ts", "datetime", "year", "month", "weekofyear", "day", "weekday", "hour")
    
    songsplay_table = logs_df.join(song_df, logs_df.song == song_df.title, how="inner")\
        .select(F.monotonically_increasing_id().alias("songsplay_id"), F.col("datetime").alias("start_time"),\
                 F.col("userid").alias("user_id"), "level", "song_id", "artist_id", F.col("sessionId").alias("session_id"),\
                      "location", F.col("userAgent").alias("user_agent"))
    
    # save tables in parquet files
    song_table.write.mode("overwrite").partitionBy(
        "year", "song_id").parquet(OutputPath)
    
    artist_table.write.parquet(OutputPath,
                               mode="overwrite", partitionBy=["artist_id", "artist_name"])
    
    users_table.write.parquet(OutputPath,
                              mode="overwrite", partitionBy=["userid", "firstName"])
    
    time_table.write.parquet(OutputPath, 
                             mode="overwrite", partitionBy=["year", "month"])
    
    songsplay_table.write.parquet(OutputPath,
                                  mode="overwrite", partitionBy=["location", "songsplay_id"])
    

def main():
    logs_InputPath = SOURCE_S3_LOGS_BUCKET
    songs_OutputPath = SOURCE_S3_SONGS_BUCKET
    OutputPath = DEST_S3_BUCKET

    # init spark session
    spark = SparkSession.builder \
        .master("local") \
        .appName("Spakify-ETL") \
        .getOrCreate()
    
    logs_df = process_logs(spark, logs_InputPath)
    song_df = process_songs(spark, songs_OutputPath)

    load_tables(spark, logs_df, song_df, OutputPath)

# RUN ETL SCRIPT
if __name__ == "__main__":
    main()