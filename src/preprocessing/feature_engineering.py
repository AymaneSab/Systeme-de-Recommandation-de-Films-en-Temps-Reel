import json
from confluent_kafka import Consumer
import pandas as pd

# Load initial data
movie_data = pd.DataFrame(...)  # Load your initial movie data
user_activity = pd.DataFrame(columns=['userId', 'user_activity'])  # Initialize empty user activity dataframe
average_ratings = pd.DataFrame(columns=['movieId', 'average_rating'])  # Initialize empty average ratings dataframe

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'user_activity_group',  # Consumer group ID
    'auto.offset.reset': 'earliest'
}

# Kafka Consumer for user reviews
consumer = Consumer(consumer_conf)
consumer.subscribe(['user_reviews_topic'])  # Replace with your Kafka topic for user reviews

try:
    while True:
        msg = consumer.poll(timeout=1000)  # Adjust timeout as needed

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        # Process the received message
        user_review = json.loads(msg.value().decode('utf-8'))

        # Update user activity
        user_id = user_review['userId']
        user_reviews = user_activity[user_activity['userId'] == user_id]
        user_activity.loc[user_reviews.index, 'user_activity'] = user_reviews.shape[0] + 1

        # Recalculate average ratings for movies
        average_ratings = review_data.groupby('movieId')['rating'].mean().reset_index(name='average_rating')

        # Update movie_data with the new average ratings
        movie_data = pd.merge(movie_data, average_ratings, on='movieId', how='left')

        # Display the updated dataframes
        print("Updated User Activity:")
        print(user_activity.head())

        print("\nUpdated Average Ratings:")
        print(average_ratings.head())

        print("\nUpdated Movie Data:")
        print(movie_data.head())

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
