import os
import json
import logging
from datetime import datetime 
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
import requests 
import time

# Setup Logging Function 
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

def setup_KafkaLoader_logging():
    return setup_logging("Log/KafkaProducer", "Api_Loader")

# Function To Create Kakfa Topics 
def create_kafka_topic(topic, admin_client, producer_logger):
    try:
        topic_spec = NewTopic(topic, num_partitions=1, replication_factor=1)

        admin_client.create_topics([topic_spec])

        separator = '-' * 30
        producer_logger.info(f"Kafka {topic} {separator} Created Successfully: ")

    except Exception as e:
        error_message = "Error creating Kafka topic: " + str(e)
        producer_logger.error(error_message)

# Function To Ingest Data Into Kafka Topics 
def produce_to_Topics(movieTopic, reviewTopic, userTopic,  producer_logger):
    try:
        producer = Producer({"bootstrap.servers": "localhost:9092"})  # Kafka broker address

        while True:
            try:
                # Use the streaming endpoint to get movie data
                movieLens_endpoint = 'http://localhost:5002/movie_data'
                response = requests.get(movieLens_endpoint, stream=True)

                for line in response.iter_lines():
                    if line:
                        if line :
                            json_data = json.loads(line)
                            
                            # Access movie, review, and user data separately
                            movie_data = json_data['movie']
                            review_data = json_data['review']
                            user_data = json_data['user']

                            try:
                                separator = '-' * 30

                                # Insert To Review Topic 
                                producer.produce(movieTopic, key="movie", value=json.dumps(movie_data))
                                producer_logger.info(f"Movie {movie_data["movieId"]} Produced Top  {movieTopic}  {separator} Succefully ")

                                # Insert to Movie Topic 
                                producer.produce(reviewTopic, key="review", value=json.dumps(review_data))
                                producer_logger.info(f"Review {review_data["timestamp"]} Produced Top  {reviewTopic} {separator} Succefully ")

                                # Insert To User Topic 
                                producer.produce(userTopic, key="user", value=json.dumps(user_data))
                                producer_logger.info(f"Review {user_data["userId"]} Produced Top {userTopic} {separator} Succefully ")

                                # Flush only if everything is successful
                                producer.flush()
                                time.sleep(2)

                            except ValueError as ve:
                                # Log the error if the date formatting fails
                                error_message = f"Error Producing To Kafka : {ve}"
                                producer_logger.error(error_message)

            except Exception as ex:
                # Log other validation errors
                error_message = f"Error Getting Data From API {ex}"
                producer_logger.error(error_message)

    except Exception as e:
        error_message = "Error In Kafka Connection  " + str(e)
        producer_logger.error(error_message)

# Exemple Usage
def runKafkaProducer(topic1, topic2 , topic3):
    
    producer_logger = setup_KafkaLoader_logging()

    try:
        producer_logger.info("Kafka Producer started -----------------------------------------------------------> Succefully ")

        # Create a Kafka admin client
        admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})

        # Check if the topics exist, and create them if not
        for topic in [topic1, topic2 , topic3 ]:
            existing_topics = admin_client.list_topics().topics
            
            if topic not in existing_topics:
                create_kafka_topic(topic, admin_client, producer_logger)

        # Start producing to both topics simultaneously
        produce_to_Topics(topic1 , topic2 , topic3 , producer_logger)

        
    except KeyboardInterrupt:
        producer_logger.info("Kafka Producer Stopped")

    except Exception as e:
        error_message = "An unexpected error occurred in Kafka Producer: " + str(e)
        producer_logger.error(error_message)

topic1 = "Movies"
topic2 = "Reviews"
topic3 = "Users"

runKafkaProducer(topic1, topic2 , topic3)
 