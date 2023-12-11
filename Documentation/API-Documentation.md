# Movie Recommendation API

This Flask-based API provides access to movie data, including user reviews and user details, from the MovieLens dataset.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Usage](#usage)
  - [GET /movie_data](#get-movie_data)

## Overview

This API is built using Flask and serves movie-related data from the MovieLens dataset. It includes functionality to retrieve movie data, including reviews and user details.

## Setup

### Requirements

- Python 3.6 or higher
- Flask
- Pandas

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/your-repository.git
   cd your-repository
   ```

## Usage
### GET /movie_data

 . Endpoint to retrieve movie data, including reviews and user details.

 -- Request

    The response is a streaming JSON containing movie, review, and user data.

    ```
    curl http://localhost:5002/movie_data

    ```
 -- Response

 ```
 {"movie": {"movieId": "1", "title": "Toy Story", "release_date": "1995-01-01", "genres": ["Animation", "Children", "Comedy"], "IMDb_URL": "http://www.imdb.com/title/tt0114709/"},
 "review": {"userId": "1", "movieId": "1", "rating": "5", "timestamp": "876893171"},
 "user": {"userId": "1", "age": "24", "gender": "M", "occupation": "technician", "zipcode": "85711"}}
```

