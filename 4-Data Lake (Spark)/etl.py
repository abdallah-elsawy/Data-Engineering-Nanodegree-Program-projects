import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """   
    Creates the Spark Session if not created 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """   
    We use this function to loads Song Data from S3 to our directory then creates songs and artist tables  
    """
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(log_data)

    # extract columns to create songs table
    songs_table = spark.sql(""" 
                                SELECT DISTINCT song_id, title, artist_id, year, duration
                                FROM songs_table
                             """) 
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs_table.parquet",mode="overwrite",partitionBy=["year", "artist_id"]) 

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                                FROM artists_table
                                """) 
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table.parquet",mode="overwrite")
    
    
    


def process_log_data(spark, input_data, output_data):
    """   
    We use this function to loads Log Data from S3 to our directory then creates song_plays and time           tables  
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    artists_table = spark.sql("""
                                SELECT DISTINCT 'userId', 'firstName', 'lastName', 'gender', 'level','ts'
                                FROM users_table
                                """) 
    
    
    # write users table to parquet files
    artists_table.write.parquet(output_data + "users_table.parquet",mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000),Timestamp())
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'), Str())
    df = df.withColumn('date', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = spark.sql("""
                                SELECT DISTINCT date,hour(timestamp),day(timestamp),weekofyear(timestamp),
                                month(timestamp) AS month,year(timestamp) AS year,dayofweek(timestamp)
                                FROM time_table
                                """) 
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time_table.parquet",mode="overwrite", partitionBy=["year", "month"]) 


    # read in song data to use for songplays table
    song_df = spark.json(input_data)
    

    # extract columns from joined song and log datasets to create songplays table 
    
    df = df.join(song_df, (song_df.artist_name == df.artist)&(song_df.song == song_df.title))
    df = df.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table = spark.sql("""
                                SELECT DISTINCT 'songplay_id','start_time', 'userId', 'level', 'song_id',                                                 'artist_id', 'sessionId', 'location', 'userAgent'
                                FROM songplays_table
                                """) 

    # write songplays table to parquet files partitioned by year and month
    songplays_tablewrite.parquet(output_data + "songplays_table.parquet",mode="overwrite", partitionBy=["year", "month"]) 


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data ="s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
