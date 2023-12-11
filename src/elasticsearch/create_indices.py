from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
from elasticsearch import Elasticsearch
from datetime import datetime
from elasticsearch.exceptions import RequestError
import os
import logging


def elastic_setup_logging():
    log_directory = "Log/ElasticSearch"

    os.makedirs(log_directory, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)

    logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    consumer_logger = logging.getLogger(__name__)  
    
    return consumer_logger

def connectToelastic(elastic_logger):
    # We can directly do all Elasticsearch-related operations in our Spark script using this object.
    es = Elasticsearch("http://localhost:9200")

    if es:
        elastic_logger.info("Connected to elastic search Successfully")

        return es
    
def createMovieIndex(esconnection, elastic_logger):
    my_index_body = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        },
        "mappings": {
            "properties": {
                "movieId": {"type": "keyword"},
                "title": {"type": "keyword"},
                "release_date": {"type": "date"},
                "genres": {"type": "keyword"},  # Change to "text" if you are indexing an array of strings directly
                "IMDb_URL": {"type": "keyword"},
                "movie_average_rating": {"type": "float"}  
            }
        }
    }

    try:
        esconnection.indices.create(index="movie", body=my_index_body)
        elastic_logger.info("Index 'movie' created successfully.")
        
    except RequestError as e:
        if "resource_already_exists_exception" in str(e):
            elastic_logger.info("Index 'movie' already exists.")
        else:
            elastic_logger.error(f"Error creating index: {e}")

def createReviewsIndex(esconnection, elastic_logger):
    my_index_body = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        },
        "mappings": {
            "properties": {
                "userId": {"type": "keyword"},
                "movieId": {"type": "keyword"},
                "rating": {"type": "float"},  # Change the type to match your actual data type
                "timestamp": {"type": "date"}  # Change the type to match your actual data type
            }
        }
    }

    try:
        esconnection.indices.create(index="review", body=my_index_body)
        elastic_logger.info("Index 'review' created successfully.")
    except RequestError as e:
        if "resource_already_exists_exception" in str(e):
            elastic_logger.info("Index 'review' already exists.")
        else:
            elastic_logger.error(f"Error creating index: {e}")

def createUserIndex(esconnection, elastic_logger):
    my_index_body = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        },
        "mappings": {
            "properties": {
                "userId": {"type": "keyword"},
                "age": {"type": "keyword"},
                "gender": {"type": "keyword"},
                "occupation": {"type": "keyword"},
                "zipcode": {"type": "text"},
                "user_activity": {"type": "float"} 
            }
        }
    }

    try:
        esconnection.indices.create(index="user", body=my_index_body)
        elastic_logger.info("Index 'user' created successfully.")
    except RequestError as e:
        if "resource_already_exists_exception" in str(e):
            elastic_logger.info("Index 'user' already exists.")
        else:
            elastic_logger.error(f"Error creating index: {e}")


