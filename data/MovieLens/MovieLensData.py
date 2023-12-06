from flask import Flask, jsonify 
import pandas as pd
import sys
import time
import logging
import json
import os 
from datetime import datetime 

# Assuming your RawData directory is under the current working directory
raw_data_directory = '/home/hadoop/Syst-me-de-Recommandation-de-Films-en-Temps-R-el-avec-Apache-Spark-Elasticsearch-Kibana-et-Flask/data/MovieLens/RawData'

u_data_path = os.path.join(raw_data_directory, 'u.data')
u_item_path = os.path.join(raw_data_directory, 'u.item')
u_user_path = os.path.join(raw_data_directory, 'u.user')

app = Flask(__name__)

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

def setup_Api_logging():
    return setup_logging("Log/API_Movies_LogFiles", "API_logger")

# Function to read data files
def read_data_files():
    try:
        u_data = pd.read_csv(u_data_path, sep='\t', names=['userId', 'movieId', 'rating', 'timestamp'])
        u_item = pd.read_csv(u_item_path, sep='|', encoding='latin-1', header=None, names=['movieId', 'title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'])
        u_user = pd.read_csv(u_user_path, sep='|', names=['userId', 'age', 'gender', 'occupation', 'zipcode'])
       
        return u_data, u_item, u_user
    
    except Exception as e:
        logging.error(f"Error reading data files: {e}")
        raise

# Function to extract genres for each movie
def extract_genres(row):
    genres = ['unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    movie_genres = [genre for genre, val in zip(genres, row[5:]) if val == 1]
    return movie_genres

# Function to create JSON entry
def create_json_entry(row):
    movie_data = {
        'movieId': str(row['movieId']),
        'title': row['title'],
        'release_date': row['release_date'],
        'video_release_date': row['video_release_date'],
        'IMDb_URL': row['IMDb_URL']
    }

    review_data = {
        'userId': str(row['userId']),
        'movieId': str(row['movieId']),
        'rating': str(row['rating']),
        'timestamp': str(row['timestamp'])
    }

    user_data = {
        'userId': str(row['userId']),
        'age': str(row['age']),
        'gender': row['gender'],
        'occupation': row['occupation'],
        'zipcode': row['zipcode']
    }

    combined_data = {'movie': movie_data, 'review': review_data, 'user': user_data}
    json_data = json.dumps(combined_data)
    
    return json_data

@app.route('/movie_data', methods=['GET'])
def get_movie_data():
    api_logger = setup_Api_logging()
    api_logger.info("Api Started")

    try:
        u_data, u_item, u_user = read_data_files()

        # Apply genre extraction function to each row in u_item
        u_item['genres'] = u_item.apply(extract_genres, axis=1)

        # Merge relevant data
        merged_data = pd.merge(u_data, u_item[['movieId', 'title', 'release_date', 'video_release_date', 'IMDb_URL']], on='movieId')
        merged_data = pd.merge(merged_data, u_user[['userId', 'age', 'gender', 'occupation', 'zipcode']], on='userId')

        # Convert to JSON format and return as a streaming response with a delay
        def generate():
            for _, row in merged_data.iterrows():
                json_data = create_json_entry(row)
                yield json_data + '\n'  # Ensure each JSON object is on a new line
                time.sleep(2)  # Introduce a delay of 2 seconds between each response
                logging.info(f"Returned message: {json_data}")

        return app.response_class(generate(), content_type='application/json')
    
    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5002)
