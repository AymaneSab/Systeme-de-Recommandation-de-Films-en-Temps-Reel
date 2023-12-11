import requests

def get_movie_poster(movie_title, api_key):
    base_url = "http://www.omdbapi.com/"
    params = {
        't': movie_title,
        'apikey': "de5e5d7a",
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    if response.status_code == 200 and data.get('Response') == 'True':
        poster_url = data.get('Poster')
        return poster_url
    else:
        print(f"Error: {data.get('Error', 'Unknown error')}")
        return None

