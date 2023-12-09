import findspark
findspark.init()

import logging
from datetime import datetime
import threading
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType , IntegerType
from pyspark.sql.streaming import StreamingQueryException
import os 
import sys 
import pandas as pd

from pyspark.sql.functions import col,from_json, explode , lit
from pyspark.sql import functions as F
from create_indices import connectToelastic, createMovieIndex, createReviewsIndex, createUserIndex
from clean_data import clean_and_preprocess_movie_data, clean_and_preprocess_review_data, clean_and_preprocess_user_data

from feature_engineering import calculate_user_average 

def setup_logging(log_directory, logger_name):
    os.makedirs(log_directory, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    handler = logging.FileHandler(log_filepath)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger

def setup_sparkSessionInitialiser_logging():
    return setup_logging("Log/Spark", "sparkSessionInitialiser")

def setup_main_logging():
    return setup_logging("Log/Main_Script", "main")

def setup_sparkTreatment_movies_logging():
    return setup_logging("Log/Kafka_Movies_Insertion", "sparkTreatment_movies")

def setup_sparkTreatment_reviews_logging():
    return setup_logging("Log/Kafka_Reviews_Insertion", "sparkTreatment_reviews_")

def setup_sparkTreatment_user_logging():
    return setup_logging("Log/Kafka_User_Insertion", "sparkTreatment_user")

def sparkSessionInitialiser(logger):

        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.4.2",
        ]
        logger.info("Packages Loaded ")

        # Initialize SparkSession for Elasticsearch
        spark = SparkSession.builder \
            .appName("Spark Treatment") \
            .config("spark.jars.packages", ",".join(packages)) \
            .getOrCreate()
        
        logger.info("Spark Session Returned")
        
        return spark

def sparkTreatment_movies(topicname, kafka_bootstrap_servers , spark_logger , movies_logger):
    try:
        
        spark = sparkSessionInitialiser(spark_logger)

        movies_logger.info("sparkTreatment_movies")

        # Define the schema for Kafka messages
        kafka_schema = StructType([
            StructField("movieId", StringType(), True),
            StructField("title", StringType(), True),
            StructField("release_date", StringType(), True),  
            StructField("genres", ArrayType(StringType()), True),  
            StructField("IMDb_URL", StringType(), True),
        ])

        # Read data from Kafka topic with defined schema
        kafka_stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topicname) \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", kafka_schema).alias("data")) \
            .select("data.*")
        
        movies_logger.info(f"Data Loaded From {topicname} Topic Succefully ")
        
        treated_movie = clean_and_preprocess_movie_data(kafka_stream_df)

        movies_logger.info("Data Treated Succefully ")

        es = connectToelastic(movies_logger)

        createMovieIndex(es , movies_logger)

        movies_logger.info("Movie Index Created Succefully")

        checkpoint_location = "Elasticsearch/Checkpoint/Movies"

        if not os.path.exists(checkpoint_location):
            os.makedirs(checkpoint_location)

        # Write to Elasticsearch
        treated_movie.writeStream \
            .format("org.elasticsearch.spark.sql") \
            .outputMode("append") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "movie/_doc") \
            .option("checkpointLocation", checkpoint_location) \
            .start().awaitTermination()
        
        movies_logger.info("treated_movie Sended To Elastic ")


    except Exception as e:
        movies_logger.error(f"An error occurred: {str(e)}")

    finally:
        # Stop SparkSession
        spark.stop()
        movies_logger.info("----------> SparkSession Stopped")
        
def sparkTreatment_reviews(topicname, kafka_bootstrap_servers , spark_logger , review_logger):
    try:

        spark = sparkSessionInitialiser(spark_logger)

        review_logger.info("----------> Packages Loaded Successfully ")

        # Define the schema for Kafka messages
        kafka_schema = StructType([
            StructField("userId", StringType(), True),
            StructField("movieId", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("timestamp", StringType(), True),
        ])

        # Read data from Kafka topic with defined schema
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topicname) \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(explode(from_json("value", ArrayType(kafka_schema))).alias("data")) \
            .select("data.*")
        
        treated_reviews = clean_and_preprocess_review_data(df)

        checkpoint_location = "Elasticsearch/Checkpoint/Reviews"

        if not os.path.exists(checkpoint_location):
            os.makedirs(checkpoint_location)

        es = connectToelastic(review_logger)

        createReviewsIndex(es , review_logger)

        # Write to Elasticsearch
        treated_reviews.writeStream \
            .format("org.elasticsearch.spark.sql") \
            .outputMode("append") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "review/_doc") \
            .option("checkpointLocation", checkpoint_location) \
            .start().awaitTermination()
        
        
        review_logger.info("treated_reviews Sended To Elastic ")


    except Exception as e:
        review_logger.error(f"An error occurred: {str(e)}")
    finally:
        spark.stop()

def sparkTreatment_user(topicname, kafka_bootstrap_servers, spark_logger, user_logger):
    try:
        # Initialize Spark session
        session = SparkSession.builder.appName("YourAppName").getOrCreate()
        user_logger.info("----------> Packages Loaded Successfully ")

        # Define the schema for Kafka messages
        kafka_schema = StructType([
            StructField("userId", StringType(), True),
            StructField("age", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("occupation", StringType(), True),
            StructField("zipcode", StringType(), True)
        ])

        # Read data from Kafka topic with defined schema
        df = session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topicname) \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(explode(from_json("value", ArrayType(kafka_schema))).alias("data")) \
            .select("data.*")

        try :

            treated_users = clean_and_preprocess_user_data(df)
            user_logger.info("Transformation Succefull")

        except Exception as e :
            user_logger.error(f"Transformation Failed {str(e)}")

        checkpoint_location = "Elasticsearch/Checkpoint/Users"

        if not os.path.exists(checkpoint_location):
            os.makedirs(checkpoint_location)

        es = connectToelastic(user_logger)
        createUserIndex(es, user_logger)

        # Write to Elasticsearch
        query = treated_users.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_location) \
            .foreachBatch(lambda df, epoch_id: write_to_elasticsearch(df, epoch_id, col("userId") ,session, user_logger)) \
            .start()

        query.awaitTermination()
        user_logger.info("treated_users Sent To Elastic ")

    except StreamingQueryException as sqe:
        user_logger.error(f"Streaming query exception: {str(sqe)}")
    except Exception as e:
        user_logger.error(f"An error occurred: {str(e)}")
    finally:
        session.stop()

def write_to_elasticsearch(df, epoch_id,userId, session, logger):
    try:
        calculate_user_average_udf = lambda user_id: calculate_user_average(userId, session, user_logger)

        treated_users = df.withColumn('user_activity', lit(calculate_user_average_udf(userId)))
        # Write the batch data to Elasticsearch
        treated_users.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "user/_doc") \
            .mode("append") \
            .save()

    except Exception as e:
        # Handle the exception (print or log the error message)
        logger.error(f"Error in write_to_elasticsearch: {str(e)}")

def runSparkTreatment(spark_logger , movies_logger , review_logger , user_logger , main_loggin):

    try:
        # Create threads for sparkTreatment_movies and sparkTreatment_reviews
        movies_thread = threading.Thread(target=sparkTreatment_movies, args=("Movies", "localhost:9092" , spark_logger , movies_logger))
        reviews_thread = threading.Thread(target=sparkTreatment_reviews, args=("Reviews", "localhost:9092" , spark_logger , review_logger))
        user_thread = threading.Thread(target=sparkTreatment_user, args=("Users", "localhost:9092" ,spark_logger, user_logger))

        # Start the threads
        movies_thread.start()
        reviews_thread.start()
        user_thread.start()

        # Wait for both threads to finish
        movies_thread.join()
        reviews_thread.join()
        user_thread.join()

    except KeyboardInterrupt:
        main_loggin.info("Spark Treatment Stopped")
    except Exception as e:
        main_loggin.error(f"An unexpected error occurred: {e}")
        main_loggin.exception("An unexpected error occurred in Spark")


spark_logger = setup_sparkSessionInitialiser_logging()  
movies_logger = setup_sparkTreatment_movies_logging()
review_logger = setup_sparkTreatment_reviews_logging()
user_logger = setup_sparkTreatment_user_logging()
main_loggin = setup_main_logging()


runSparkTreatment(spark_logger , movies_logger ,review_logger , user_logger , main_loggin)       
