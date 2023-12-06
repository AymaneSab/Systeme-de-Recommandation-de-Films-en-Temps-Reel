import json
import logging
from datetime import datetime
import os
from elasticsearch import Elasticsearch

from pyspark.sql.types import StructType, StructField, StringType 
from pyspark.sql.functions import from_json , col  , count

def setup_logging():
    log_directory = "Log/Feauture_Engineering"
    os.makedirs(log_directory, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)

    logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    consumer_logger = logging.getLogger(__name__)
    return consumer_logger

def calculate_user_average(spark, kafka_bootstrap_servers, user_id):
    # Read reviews for the specific user from Kafka Reviews topic
    user_reviews_count = get_reviews_count_from_kafka(spark, kafka_bootstrap_servers, user_id)

    return user_reviews_count

def get_reviews_count_from_kafka(spark, kafka_bootstrap_servers, user_id):
    # Define the schema for Kafka messages
    kafka_schema = StructType([
        StructField("userId", StringType(), True),
        StructField("movieId", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    # Read data from Kafka Reviews topic with defined schema
    kafka_reviews_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "Reviews") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", kafka_schema).alias("data")) \
        .select("data.*") \
        .filter(col("userId") == user_id)

    # Get the count of user reviews without reading the entire data
    reviews_count = kafka_reviews_df.agg(count("*").alias("reviews_count")).collect()[0]['reviews_count']

    return reviews_count

