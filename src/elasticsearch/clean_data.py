from pyspark.sql.functions import col, lit, from_unixtime, coalesce , to_date , date_format
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def clean_and_preprocess_movie_data(kafka_stream_df):
    try:
        # Convert 'movieId' to string
        transformed_df = kafka_stream_df.withColumn("movieId", col("movieId").cast("string"))



        # Assuming "release_date" is a string column in the format '01-Jan-1995'
        transformed_df = transformed_df.withColumn("release_date", coalesce(to_date("release_date", 'dd-MMM-yyyy'), col("release_date")))

        # Format the date as 'yyyy-MM-dd'
        transformed_df = transformed_df.withColumn("release_date", date_format("release_date", "yyyy-MM-dd"))

        transformed_df = transformed_df.withColumn("IMDb_URL", coalesce("IMDb_URL", lit("No Image Provided")))

        # Convert 'movie_average_rating' to float
        transformed_df = transformed_df.withColumn("movie_average_rating", lit(1.0).cast("float"))

        return transformed_df

    except ValueError:
        # Handle the case where the date format is incorrect
        return None
    
def clean_and_preprocess_review_data(kafka_stream_df):
    try:

        # Convert 'rating' to float and handle any potential errors
        df = kafka_stream_df.withColumn("rating", col("rating").cast("float"))

        # Perform Min-Max normalization for 'rating' 
        min_rating = 0
        max_rating = 10

        df = df.withColumn("rating", (col("rating") - min_rating) / (max_rating - min_rating))

        # Convert 'timestamp' to datetime format
        df = df.withColumn("timestamp", date_format(from_unixtime(col("timestamp").cast("bigint")), "yyyy-MM-dd"))
        # Convert data types if needed
        df = df.withColumn("userId", col("userId").cast("string"))
        df = df.withColumn("movieId", col("movieId").cast("string"))

        return df

    except ValueError:
        # Handle the case where the conversion to float fails
        return df.withColumn("rating", lit(0.0))

def clean_and_preprocess_user_data(kafka_stream_df):
    try:

        # Handle missing values (replace 'your_default_value' with an appropriate default)
        transformed_df = kafka_stream_df.withColumn("age", col("age").cast("integer"))

        transformed_df = transformed_df.withColumn("gender", col("gender").cast("string"))
        
        transformed_df = transformed_df.withColumn("occupation", col("occupation").cast("string"))
        transformed_df = transformed_df.withColumn("zipcode", col("zipcode").cast("string"))

        # Convert data types if needed
        transformed_df = transformed_df.withColumn("userId", col("userId").cast("string"))

        # Add a new field 'user_activity' with a default value of 1.0
        transformed_df = transformed_df.withColumn("user_activity", lit(1.0).cast("float"))    

        return transformed_df

    except ValueError:
        # Handle any potential errors
        return kafka_stream_df.withColumn("user_activity", lit(0.0).cast("float"))

