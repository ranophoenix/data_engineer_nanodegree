import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

INPUT_BUCKET = config.get('S3', 'INPUT_BUCKET')
OUTPUT_BUCKET = config.get('S3', 'OUTPUT_BUCKET')


def create_spark_session():
    """Creates a spark session supporting S3.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes song-data json files to populate songs and artists tables.
    
    Args:
        spark: A spark session.
        input_data: S3 bucket containing the files to load
        output_data: S3 bucket to copy the processed files to.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.option("inferSchema", False).json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
               .mode("overwrite") \
               .partitionBy('year', 'artist_id') \
               .parquet(path=output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', \
                              'artist_latitude', 'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write \
                 .mode("overwrite") \
                 .parquet(path=output_data + 'artists')                 
    
    df.createOrReplaceTempView("tbl_song_data")


def process_log_data(spark, input_data, output_data):
    """Processes log-data json files to populate users, time and songplays tables.
    
    Args:
        spark: A spark session.
        input_data: S3 bucket containing the files to load
        output_data: S3 bucket to copy the processed files to.
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(col('page') == 'NextSong')


    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').drop_duplicates(subset=['userId'])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(path=output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("timestamp")) \
                    .withColumn("day", dayofmonth("timestamp")) \
                    .withColumn("month", month("timestamp")) \
                    .withColumn("year", year("timestamp")) \
                    .withColumn("week", weekofyear("timestamp")) \
                    .withColumn("weekday", dayofweek("timestamp"))
    
    time_table = df.select('start_time', 'weekday', 'year', 'month',\
                           'week', 'day', 'hour').distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write \
                .mode('overwrite') \
                .partitionBy('year', 'month') \
                .parquet(path=output_data + 'time')
    

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM tbl_song_data")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner") \
                        .distinct() \
                        .select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId','location','userAgent',\
                                'year', 'month')\
                        .withColumn("songplay_id", monotonically_increasing_id())


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
                    .mode("overwrite") \
                    .partitionBy('year', 'month') \
                    .parquet(path=output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = INPUT_BUCKET
    output_data = OUTPUT_BUCKET
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
