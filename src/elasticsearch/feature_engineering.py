from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StructField, StringType , IntegerType

def calculate_user_average(user_id, session, logger):
    try:

        logger.info(f"Duser_id : {user_id}")

        # Define the schema for Kafka messages
        kafka_schema = StructType([
            StructField("userId", StringType(), True),
            StructField("movieId", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("timestamp", StringType(), True),
        ])

        # Read data from Kafka Reviews topic with defined schema
        kafka_reviews_df = session \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "Reviews") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", kafka_schema).alias("data")) \
            .select("data.*") \
            .filter(col("userId") == user_id)

        logger.info("Data Readed Successfully, Future Func {kafka_reviews_df}")

        # Get the count of user reviews without reading the entire data
        reviews_count = kafka_reviews_df.agg(count("*").alias("reviews_count")).collect()[0]['reviews_count']
        

        logger.info(f"----------------{reviews_count}")

        return float(reviews_count)

    except Exception as e:
        # Handle the exception (print or log the error message, and return a default value)
        logger.error(f"Error in get_reviews_count_from_kafka: {str(e)}")
        raise e
