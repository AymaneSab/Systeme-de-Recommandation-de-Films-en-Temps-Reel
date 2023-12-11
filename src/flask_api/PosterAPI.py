import requests

def get_movie_poster(movie_title):
    # Extracting only the film name from the provided movie title
    film_name = movie_title.split('(')[0].strip()

    base_url = "http://www.omdbapi.com/"
    params = {
        't': film_name,
        'apikey': "de5e5d7a",
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    if response.status_code == 200 and data.get('Response') == 'True':
        poster_url = data.get('Poster')
        return poster_url
    else:
        print(f"Error: {data.get('Error', 'Unknown error')}")
        return "https://cdn-icons-png.flaticon.com/512/2589/2589327.png"
    


