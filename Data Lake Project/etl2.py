import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import *
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get['AWS', 'AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get['AWS',
                                                 'AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description:
        Process the songs data files and create extract songs table and artist table data from it.

    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', "*.json")

    songSchema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_name", Str()),
        Fld("duration", Dbl()),
        Fld("num_songs", Int()),
        Fld("title", Str()),
        Fld("year", Int()),
    ])

    # read song data file
    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    songs_table = df.selectExpr(
        ["song_id", "title", "artist_id", "year", "duration"]).drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        output_data + "songs/", mode="overwrite", partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", \                "artist_longitude as longitude"]).drop_duplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Description:
            Process the event log file and extract data for table time, users and songplays from it.

    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, "log-data/")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df.selectExpr(
        ["userId", "firstName", "lastName", "gender", "level"]).drop_duplicates()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(
        output_data, "users/"), mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(
        int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))

    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time"))\
        .withColumn("day", dayofmonth("start_time"))\
        .withColumn("week", weekofyear("start_time"))\
        .withColumn("month", month("start_time"))\
        .withColumn("year", year("start_time"))\
        .withColumn("weekday", dayofweek("start_time"))\
        .select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday").drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(
        output_data, "time_table/"), mode='overwrite', partitionBy=["year", "month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/*/*/*')

    """
    
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner')\
         .select(monotonically_increasing_id().alias("songplay_id"), col("start_time").alias('start_time'),col("userId").alias("user_id"),            "level", "song_id", "artist_id", col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent"))

    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month")
    """

    # extract columns from joined song and log datasets to create songplays table
    df = df.join(song_df, (song_df.title == df.song)
                 & (song_df.artist_name == df.artist))
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    songplays_table = df['songplay_id', 'start_time', 'userId', 'level',
                         'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']

    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.parquet(os.path.join(output_data, "songplays/"),
                                                    mode="overwrite", partitionBy=["year",                           "month"])


def main():
    spark = create_spark_session()
    input_data = "s3://mybucketnaija/song_data/"
    output_data = "s3://mybucketnaija/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
