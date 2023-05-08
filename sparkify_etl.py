# import libs
import configparser
import os

# spark imports and configurations
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, FloatType, StringType
from udf import timestamp_udf

# Configrations to connect with AWS services
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']

SOURCE_S3_LOGS_BUCKET = config['SOURCE_S3_LOGS_BUCKET']
SOURCE_S3_SONGS_BUCKET = config['SOURCE_S3_SONGS_BUCKET']
DEST_S3_BUCKET = config['DEST_S3_BUCKET']


def process_songs(spark, input_path):
    """
    Description: process_songs function
    - Function to import song files using input_path, clean the data in song files,
        and Extract features to load it to the analytical tables files
    - ETL part of the songs files

    Parameters:
    spark: created spark session to use within the function
    input_path: path or URL of input files
    """
    # read song files
    song_df_schema = StructType([
        StructField("num_songs", IntegerType),
        StructField("artist_id", StringType),
        StructField("artist_latitude", DoubleType),
        StructField("artist_longitude", DoubleType),
        StructField("artist_location", StringType),
        StructField("artist_name", StringType),
        StructField("song_id", StringType),
        StructField("title", StringType),
        StructField("duration", FloatType),
        StructField("year", IntegerType)])

    song_df = spark.read.json(
        input_path, schema=song_df_schema, columnNameOfCorruptRecord='corrupt_record')

    # clean
    song_df = song_df.na.drop()

    return song_df


def process_logs(spark, input_path):
    """
    Description: process_logs function
    - Function to import log files using input_path, clean the data in log files,
        and Extract features to load it to the analytical tables files
    - ETL part of the logs files

    Parameters:
    spark: created spark session to use within the function
    input_path: path or URL of input files
    """
    # read the logs
    logs_df_schema = StructType([
        StructField("artist", StringType),
        StructField("auth", StringType),
        StructField("firstName", StringType),
        StructField("gender", StringType),
        StructField("itemInSession", IntegerType),
        StructField("lastName", StringType),
        StructField("length", FloatType),
        StructField("level", StringType),
        StructField("location", StringType),
        StructField("method", StringType),
        StructField("page", StringType),
        StructField("registration", DoubleType),
        StructField("sessionId", IntegerType),
        StructField("song", StringType),
        StructField("status", IntegerType),
        StructField("ts", DoubleType),
        StructField("userAgent", StringType),
        StructField("userId", IntegerType)])

    logs_df = spark.read.json(
        input_path, schema=logs_df_schema, columnNameOfCorruptRecord='corrupt_record')

    # filter the data
    logs_df = logs_df.filter(logs_df.page == "NextSong")

    # clean
    logs_df = logs_df.drop_duplicates(["userid"])
    logs_df = logs_df.na.drop()

    # convert Time stamp and extract datetime features
    logs_df = logs_df.withColumn("timestamp", timestamp_udf(F.col("ts")))

    logs_df = logs_df.withColumn("year", F.year("timestamp"))
    logs_df = logs_df.withColumn("month", F.month("timestamp"))
    logs_df = logs_df.withColumn("weekofyear", F.weekofyear("timestamp"))
    logs_df = logs_df.withColumn("day", F.dayofmonth("timestamp"))
    logs_df = logs_df.withColumn("hour", F.hour("timestamp"))
    logs_df = logs_df.withColumn("weekday", F.dayofweek("timestamp"))

    return logs_df


def load_tables(spark, logs_df, song_df, output_path):
    """
    Description: load_tables function
    - Function to load logs_df. and song_df, and select features to load it to the analytical tables files

    Parameters:
    logs_df: cleansed logs_df
    song_df: cleansed song_df
    output_path: path or URL to locations of saved analytical tables
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
        .select(F.monotonically_increasing_id().alias("songsplay_id"), F.col("timestamp").alias("start_time"),
                F.col("userid").alias("user_id"), "level", "song_id", "artist_id", F.col(
                    "sessionId").alias("session_id"),
                "location", F.col("userAgent").alias("user_agent"))

    # save tables in parquet files
    song_table.write.mode("overwrite").partitionBy(
        "year", "song_id").parquet(output_path)

    artist_table.write.parquet(output_path,
                               mode="overwrite", partitionBy=["artist_id", "artist_name"])

    users_table.write.parquet(output_path,
                              mode="overwrite", partitionBy=["userid", "firstName"])

    time_table.write.parquet(output_path,
                             mode="overwrite", partitionBy=["year", "month"])

    songsplay_table.write.parquet(output_path,
                                  mode="overwrite", partitionBy=["location", "songsplay_id"])


def main():
    """
    main function to run ETL pipeline
    """
    logs_input_path = SOURCE_S3_LOGS_BUCKET
    songs_output_path = SOURCE_S3_SONGS_BUCKET
    output_path = DEST_S3_BUCKET

    # init spark session
    spark = SparkSession.builder \
        .master("local") \
        .appName("Spakify-ETL") \
        .getOrCreate()

    logs_df = process_logs(spark, logs_input_path)
    song_df = process_songs(spark, songs_output_path)

    load_tables(spark, logs_df, song_df, output_path)


# RUN ETL SCRIPT
if __name__ == "__main__":
    main()
