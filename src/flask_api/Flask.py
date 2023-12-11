from flask import Flask, render_template, jsonify, request
from elasticsearch import Elasticsearch
import sys 
from pyspark.sql import SparkSession


sys.path.append("/home/hadoop/Syst-me-de-Recommandation-de-Films-en-Temps-R-el-avec-Apache-Spark-Elasticsearch-Kibana-et-Flask/src/model/")

from als_model import get_recommendations_for_movie

# Load the saved model in another script
from pyspark.ml.recommendation import ALSModel


spark = SparkSession.builder.appName("FlaskAPI").getOrCreate()

best_model = ALSModel.load("/home/hadoop/Syst-me-de-Recommandation-de-Films-en-Temps-R-el-avec-Apache-Spark-Elasticsearch-Kibana-et-Flask/src/model/best_model")

app = Flask(__name__)
es = Elasticsearch("http://localhost:9200")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/recommend', methods=['POST'])
def get_recommendations():
    try:
        data = request.get_json()
        film_name = data.get('film_name', "")

        # Use your existing Elasticsearch instance
        es = Elasticsearch("http://localhost:9200")  # Replace with your Elasticsearch URL

        # Get movie recommendations using the ALS model
        movie_ids = get_recommendations_for_movie(es, spark , best_model, film_name)

        if not movie_ids:
            return jsonify({'info': f'No Recommendations for the film "{film_name}".'})

        recommendations = []

        # Fetch movie information for each recommended movie ID
        for movie_id in movie_ids:
            movie_query = {
                "query": {
                    "term": {
                        "movieId": movie_id
                    }
                }
            }

            movie_result = es.search(index='movie', body=movie_query)

            if movie_result['hits']['hits']:
                movie_info = movie_result['hits']['hits'][0]['_source']
                recommendations.append({
                    'title': movie_info['title'],
                    'release_date': movie_info['release_date'],
                    'genres': movie_info['genres']
                })

        if not recommendations:
            return jsonify({'info': f'No Recommendations for the film "{film_name}".'})
        else:
            return jsonify({'recommendations': recommendations})

    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500

if __name__ == '__main__':
    app.run(debug=True)

@app.route('/autocomplete', methods=['POST'])
def autocomplete():
    try:
        data = request.get_json()
        prefix = data.get('prefix', "")

        # Elasticsearch prefix query for autocomplete
        autocomplete_query = {
            "query": {
                "prefix": {
                    "title.keyword": prefix.lower()
                }
            },
            "_source": ["title"],
            "size": all  
        }

        autocomplete_result = es.search(index='movie', body=autocomplete_query)

        suggestions = [hit['_source']['title'] for hit in autocomplete_result['hits']['hits']]
        
        return jsonify({'suggestions': suggestions})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
