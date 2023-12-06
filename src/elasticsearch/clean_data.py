from pyspark.sql.functions import col, from_json, lit, from_unixtime, coalesce , to_date

def clean_and_preprocess_movie_data(kafka_stream_df , schema):
    try:
        # Extract data from the 'data' column of the DataFrame
        transformed_df = kafka_stream_df.withColumn("data", from_json("value", schema)).select("data.*")

        # Convert 'release_date' and 'video_release_date' to datetime objects
        transformed_df = transformed_df.withColumn("release_date", to_date("release_date", 'yyyy-MM-dd')) \
                                       .withColumn("video_release_date", to_date("video_release_date", 'yyyy-MM-dd'))

        # Convert 'movie_average_rating' to float
        transformed_df = transformed_df.withColumn("movie_average_rating", col("movie_average_rating").cast("float"))

        # Add a default value for 'IMDb_URL' if not present
        transformed_df = transformed_df.withColumn("IMDb_URL", coalesce("IMDb_URL", lit("No Image Provided")))

        # Convert 'movieId' to string
        transformed_df = transformed_df.withColumn("movieId", col("movieId").cast("string"))

        return transformed_df

    except ValueError:
        # Handle the case where the date format is incorrect
        return None
    
def clean_and_preprocess_review_data(kafka_stream_df , schema):
    try:

        df = kafka_stream_df.withColumn("data", from_json("value", schema)).select("data.*")

        # Convert 'rating' to float and handle any potential errors
        df = df.withColumn("rating", col("rating").cast("float"))

        # Perform Min-Max normalization for 'rating' 
        min_rating = 0
        max_rating = 5

        df = df.withColumn("rating", (col("rating") - min_rating) / (max_rating - min_rating))

        # Convert 'timestamp' to datetime format
        df = df.withColumn("timestamp", from_unixtime(col("timestamp").cast("bigint")).cast("timestamp"))

        # Convert data types if needed
        df = df.withColumn("userId", col("userId").cast("string"))
        df = df.withColumn("movieId", col("movieId").cast("string"))

        return df

    except ValueError:
        # Handle the case where the conversion to float fails
        return df.withColumn("rating", lit(0.0))

def clean_and_preprocess_user_data(kafka_stream_df , schema):
    try:

        # Extract data from the 'data' column of the DataFrame
        transformed_df = kafka_stream_df.withColumn("data", from_json("value", schema)).select("data.*")

        # Handle missing values (replace 'your_default_value' with an appropriate default)
        transformed_df = transformed_df.withColumn("age", col("age").cast("integer"))
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

