from datetime import datetime 

def clean_and_preprocess_movie_data(row):
    movie_data = row
    
    try:
        movie_data['movieId'] = str(movie_data['movieId'])

        # Convert 'release_date' to a datetime object
        movie_data['release_date'] = datetime.strptime(movie_data['release_date'], '%Y-%m-%d')
        movie_data['video_release_date'] = datetime.strptime(movie_data['video_release_date'], '%Y-%m-%d')
        movie_data['IMDb_URL'] = movie_data.get('IMDb_URL', 'No Image Provided')

    except ValueError:
        # Handle the case where the date format is incorrect
        movie_data['release_date'] = None  
        movie_data['video_release_date'] = None

    # Add a new field 'movie_average_rating' with a default value of 1
    movie_data['movie_average_rating'] = 1.0

    return movie_data

def clean_and_preprocess_review_data(row):
    # Extract data from the JSON
    review_data = row

    # Perform Min-Max normalization for 'rating' 
    min_rating = 0
    max_rating = 5

    # Convert 'rating' to float and handle any potential errors
    try:
        rating = float(review_data['rating'])
        review_data['rating'] = (rating - min_rating) / (max_rating - min_rating)

        timestamp_date = datetime.fromtimestamp(int(review_data['timestamp']))
        review_data['timestamp'] = timestamp_date.strftime('%Y-%m-%d %H:%M:%S')

        # Convert data types if needed
        review_data['userId'] = str(review_data['userId'])
        review_data['movieId'] = str(review_data['movieId'])

    except ValueError:
        # Handle the case where the conversion to float fails
        review_data['rating'] = 0
    
    return review_data

def clean_and_preprocess_user_data(row):
    # Extract data from the JSON
    user_data = row

    # Handle missing values (replace 'your_default_value' with an appropriate default)
    user_data['age'] = user_data.get('age')
    user_data['gender'] = user_data.get('gender')
    user_data['occupation'] = user_data.get('occupation')
    user_data['zipcode'] = user_data.get('zipcode')

    # Convert data types if needed
    user_data['userId'] = str(user_data['userId'])

    # Add a new field 'movie_average_rating' with a default value of 1
    user_data['user_activity'] = 1.0

    # Perform additional cleaning and preprocessing if needed
    
    return user_data
