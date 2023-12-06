import findspark
findspark.init()

import logging
import os
from datetime import datetime
import threading

from create_indices import elastic_setup_logging, connectToelastic, createMovieIndex, createReviewsIndex, createUserIndex

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, date_format, to_date
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType

from clean_data import clean_and_preprocess_movie_data, clean_and_preprocess_review_data, clean_and_preprocess_user_data


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
    return setup_logging("Log/Data_Ingest", "sparkSessionInitialiser")

def setup_main_logging():
    return setup_logging("Log/Data_Ingest", "main")

def setup_sparkTreatment_movies_logging():
    return setup_logging("Log/Data_Ingest", "sparkTreatment_movies")

def setup_sparkTreatment_reviews_logging():
    return setup_logging("Log/Data_Ingest", "sparkTreatment_reviews_")

def setup_sparkTreatment_user_logging():
    return setup_logging("Log/Data_Ingest", "sparkTreatment_user")

def sparkSessionInitialiser():
        logger = setup_sparkSessionInitialiser_logging()

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

def sparkTreatment_movies(topicname, kafka_bootstrap_servers):
    try:
        logger = setup_sparkTreatment_movies_logging()

        spark = sparkSessionInitialiser()

        logger.info("sparkTreatment_movies")

        # Define the schema for Kafka messages
        kafka_schema = StructType([
            StructField("movieId", StringType(), True),
            StructField("title", StringType(), True),
            StructField("release_date", StringType(), True),  
            StructField("video_release_date", StringType(), True),  
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
        
        logger.info(f"Data Loaded From {topicname} Topic Succefully ")
        
        treated_movie = clean_and_preprocess_movie_data(kafka_stream_df)

        logger.info("Data Treated Succefully ")

        es = connectToelastic(logger)

        createMovieIndex(es , logger)

        logger.info("Movie Index Created Succefully")

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
        
        logger.info("treated_movie Sended To Elastic ")


    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

    finally:
        # Stop SparkSession
        spark.stop()
        logger.info("----------> SparkSession Stopped")
        
def sparkTreatment_reviews(topicname, kafka_bootstrap_servers):
    try:
        logger = setup_sparkTreatment_reviews_logging()

        spark = sparkSessionInitialiser()

        logger.info("----------> Packages Loaded Successfully ")

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

        es = connectToelastic(logger)

        createReviewsIndex(es , logger)

        # Write to Elasticsearch
        treated_reviews.writeStream \
            .format("org.elasticsearch.spark.sql") \
            .outputMode("append") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "review/_doc") \
            .option("checkpointLocation", checkpoint_location) \
            .start().awaitTermination()
        
        
        logger.info("treated_reviews Sended To Elastic ")


    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        spark.stop()

def sparkTreatment_user(topicname, kafka_bootstrap_servers):
    try:
        logger = setup_sparkTreatment_user_logging()

        spark = sparkSessionInitialiser()

        logger.info("----------> Packages Loaded Successfully ")

        # Define the schema for Kafka messages
        kafka_schema = StructType([
            StructField("userId", StringType(), True),
            StructField("age", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("occupation", StringType(), True),
            StructField("zipcode", StringType(), True)
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
        
        treated_users = clean_and_preprocess_user_data(df)

        checkpoint_location = "Elasticsearch/Checkpoint/Users"

        if not os.path.exists(checkpoint_location):
            os.makedirs(checkpoint_location)

        es = connectToelastic(logger)

        createUserIndex(es , logger)

        # Write to Elasticsearch
        treated_users.writeStream \
            .format("org.elasticsearch.spark.sql") \
            .outputMode("append") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "user/_doc") \
            .option("checkpointLocation", checkpoint_location) \
            .start().awaitTermination()

        logger.info("treated_users Sended To Elastic ")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        spark.stop()

# Example usage
def runSparkTreatment():
    logger = setup_main_logging()
    try:
        # Create threads for sparkTreatment_movies and sparkTreatment_reviews
        movies_thread = threading.Thread(target=sparkTreatment_movies, args=("Movies", "localhost:9092" ))
        reviews_thread = threading.Thread(target=sparkTreatment_reviews, args=("Reviews", "localhost:9092" ))
        user_thread = threading.Thread(target=sparkTreatment_user, args=("Users", "localhost:9092" ))

        # Start the threads
        movies_thread.start()
        reviews_thread.start()
        user_thread.start()

        # Wait for both threads to finish
        movies_thread.join()
        reviews_thread.join()
        user_thread.join()

    except KeyboardInterrupt:
        logger.info("Spark Treatment Stopped")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        logger.exception("An unexpected error occurred in Spark")

runSparkTreatment()