# Kafka Data Ingestion Script Documentation

## Overview

This script is designed to ingest movie-related data (movies, reviews, and users) from a streaming API into Kafka topics. It utilizes the Confluent Kafka Python library for Kafka interactions and requests library for API communication.

![Alt text](https://github.com/AymaneSab/Systeme-de-Recommandation-de-Films-en-Temps-Reel/blob/main/Documentation/Captures/Screen%20Shot%202023-12-12%20at%2009.55.17.png)
## Prerequisites

Before running the script, ensure the following:

- Kafka broker is running on `localhost:9092`.
- The streaming API (`http://localhost:5002/movie_data`) is accessible.

## Dependencies

- `os`
- `json`
- `logging`
- `datetime`
- `confluent_kafka` (Producer, AdminClient, NewTopic)
- `requests`
- `time`

## Logging

The script uses a custom logging setup. Logs are stored in the "Log/KafkaProducer" directory. Each log file is named with a timestamp (e.g., "2023-12-11_14-30-00.log").

## Functions

### 1. setup_logging(log_directory, logger_name)

   - Set up logging configuration.
   - Parameters:
     - `log_directory`: Directory to store log files.
     - `logger_name`: Name of the logger.
   - Returns: Configured logger.

### 2. setup_KafkaLoader_logging()

   - Set up logging specifically for the Kafka Producer.

### 3. create_kafka_topic(topic, admin_client, producer_logger)

   - Create a Kafka topic if it doesn't exist.
   - Parameters:
     - `topic`: Name of the Kafka topic.
     - `admin_client`: Kafka AdminClient instance.
     - `producer_logger`: Logger for producer-specific logs.

### 4. produce_to_Topics(movieTopic, reviewTopic, userTopic, producer_logger)

   - Continuously fetch movie data from the streaming API and produce it to respective Kafka topics.
   - Parameters:
     - `movieTopic`: Kafka topic for movie data.
     - `reviewTopic`: Kafka topic for review data.
     - `userTopic`: Kafka topic for user data.
     - `producer_logger`: Logger for producer-specific logs.

### 5. runKafkaProducer(topic1, topic2, topic3)

   - Main function to orchestrate the Kafka producer.
   - Parameters:
     - `topic1`: Kafka topic for movies.
     - `topic2`: Kafka topic for reviews.
     - `topic3`: Kafka topic for users.

## Usage

1. Set the Kafka broker address in the `Producer` instantiation.
2. Set the streaming API endpoint in `movieLens_endpoint` variable.
3. Set the topics for movies, reviews, and users (`topic1`, `topic2`, `topic3`).
4. Run the script.

```python
topic1 = "Movies"
topic2 = "Reviews"
topic3 = "Users"

runKafkaProducer(topic1, topic2, topic3)
