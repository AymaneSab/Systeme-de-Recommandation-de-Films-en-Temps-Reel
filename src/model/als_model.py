# set the environment path to find Recommenders
from datetime import datetime
import pandas as pd
import numpy as np
import seaborn as sns
import pandas as pd
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
import pyspark.sql.functions as F
from pyspark.sql.functions import col 
from pyspark.ml.tuning import CrossValidator
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import FloatType, IntegerType, LongType

from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.evaluation import RegressionMetrics


from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from pyspark.sql.functions import expr


def get_movie_id_by_title(es, index_name, movie_title):
    query = {
        "query": {
            "match": {
                "title": movie_title
            }
        }
    }

    # Use scan to get all documents matching the query
    results = scan(es, index=index_name, query=query)

    for result in results:
        return result["_source"]["movieId"]

    return None

def get_users_ids_for_movie_review(es, index_name, movie_id):
    query = {
        "query": {
            "term": {
                "movieId": movie_id
            }
        }
    }

    # Use scan to get all documents matching the query
    results = scan(es, index=index_name, query=query)

    user_ids = set()
    for result in results:
        user_ids.add(result["_source"]["userId"])

    return list(user_ids)

def get_recommendations_for_movie(es, spark ,best_model, movie_title, num_recommendations=5):
    # Step 1: Get movieId for the given movie title
    movie_index = "movie"
    movie_id = get_movie_id_by_title(es, movie_index, movie_title)

    if movie_id is None:
        print(f"Movie with title '{movie_title}' not found.")
        return None

    review_index = "review"
    # Step 2: Get user ids that have reviewed the movie
    user_ids = get_users_ids_for_movie_review(es, review_index, movie_id)

    if not user_ids:
        print(f"No user reviews found for the movie with title '{movie_title}'.")
        return None

    # Step 3: Create a DataFrame with user and movie data
    user_movie_data = [(int(user_id),) for user_id in user_ids]
    schema = StructType([StructField("userId", IntegerType(), True)])
    user_movie_df = spark.createDataFrame(user_movie_data, schema)

    # Step 4: Use ALS model to get recommendations for the users
    recommendations = best_model.recommendForUserSubset(user_movie_df, num_recommendations)

    # Extract movie IDs from recommendations DataFrame
    movie_ids = [movie.movieId for row in recommendations.rdd.collect() for movie in row.recommendations]
    
    return movie_ids

