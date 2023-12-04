# Système de Recommandation de Films en Temps Réel

![Alt text](https://maghreb.simplonline.co/_next/image?url=https%3A%2F%2Fsimplonline-v3-prod.s3.eu-west-3.amazonaws.com%2Fmedia%2Fimage%2Fpng%2Fcapture-656ca181b6f35308504279.png&w=1280&q=75)

## Project Structure 
```
project-root/
|-- .github/
|   |-- workflows/
|       |-- ci-cd.yml
|
|-- data/
|   |-- MovieLens/
|       |-- ml-.../ (Ensemble de données MovieLens)
|
|-- notebooks/
|   |-- exploration.ipynb
|
|-- src/
|   |-- preprocessing/
|   |   |-- __init__.py
|   |   |-- load_data.py
|   |   |-- clean_data.py
|   |   |-- feature_engineering.py
|
|   |-- model/
|   |   |-- __init__.py
|   |   |-- als_model.py
|
|   |-- elasticsearch/
|   |   |-- __init__.py
|   |   |-- create_indices.py
|   |   |-- transform_data.py
|   |   |-- ingest_data.py
|
|   |-- kibana/
|   |   |-- __init__.py
|   |   |-- create_dashboards.py
|
|   |-- flask_api/
|   |   |-- __init__.py
|   |   |-- app.py
|
|-- deployment/
|   |-- Dockerfile
|   |-- docker-compose.yml
|
|-- config/
|   |-- spark_config.json
|   |-- elasticsearch_config.json
|
|-- requirements.txt
|-- README.md
|-- .gitignore
|-- venv/
|-- tests/
|   |-- test_preprocessing.py
|   |-- test_model.py
|   |-- test_elasticsearch.py
|   |-- test_flask_api.py

```

## Objectif du Projet

Ce projet a pour objectif de concevoir une solution complète pour un système de recommandation de films en temps réel en utilisant des technologies telles qu'Apache Spark, Elasticsearch, Kibana et Flask. L'ensemble du processus couvre le traitement de données, l'apprentissage automatique, la visualisation et l'interaction via une API.

## Compréhension du Projet

### Objectif
Le but ultime est de créer un système de recommandation de films hautement performant et en temps réel, offrant une expérience utilisateur optimale.

### Ressources
L'analyse sera basée sur l'ensemble de données MovieLens, une ressource bien établie pour de tels projets.

## Configuration de l'Environnement

### Installation
Les technologies clés telles qu'Apache Spark, Elasticsearch, Kibana et Flask doivent être correctement installées et configurées.

### Acquisition des Données
L'ensemble de données MovieLens doit être téléchargé et prêt pour l'analyse.

## Prétraitement des Données avec Apache Spark

### Chargement des Données
L'ensemble de données MovieLens sera chargé dans Apache Spark pour une manipulation efficace et rapide.

### Nettoyage des Données
Les données seront nettoyées, gérant les valeurs manquantes et effectuant des conversions de types de données. Le champ des genres sera divisé en une liste pour une meilleure analyse dans Elasticsearch.

### Ingénierie des Caractéristiques
Des caractéristiques pertinentes seront créées, telles que la mesure de l'activité de l'utilisateur, les évaluations moyennes des films, et les notes moyennes fusionnées avec le DataFrame des films pour inclure les détails du film.

## Construction du Modèle de Recommandation

### Sélection du Modèle
Le modèle ALS (Alternating Least Squares) sera sélectionné pour son efficacité dans les recommandations de films.

### Entraînement du Modèle
Le modèle sera entraîné en utilisant le jeu de données préparé, suivi d'une évaluation approfondie de ses performances.

## Intégration des Données dans Elasticsearch

### Création d'Indices
Des indices Elasticsearch seront définis et créés pour les données de films et d'utilisateurs.

### Transformation des Données
Les données seront transformées pour l'ingestion optimale dans Elasticsearch.

### Ingestion des Données
Les données traitées seront ingérées dans Elasticsearch pour permettre une recherche et une récupération efficaces.

## Visualisation avec Kibana

### Création de Tableaux de Bord
Des tableaux de bord et des visualisations interactives seront créés dans Kibana, mettant en évidence des métriques telles que les évaluations moyennes des films et les niveaux d'activité des utilisateurs.

### Interprétation des Données
Des insights significatifs seront interprétés à partir des visualisations pour orienter les décisions stratégiques.

## Développement de l'API Flask

Une API robuste sera développée pour recevoir les titres de films des utilisateurs. Cette API recherchera les utilisateurs qui ont interagi avec le film spécifié, utilisant ces informations pour générer des recommandations de manière efficace.

## Déploiement

L'application Flask sera déployée sur une plateforme cloud, garantissant une disponibilité maximale. Des services tels que Heroku peuvent être utilisés pour simplifier le processus de déploiement.

## Conformité au RGPD et Gouvernance des Données

La conformité au RGPD sera assurée, avec un accent particulier sur le consentement des utilisateurs et la protection des données personnelles. Des politiques de gouvernance des données seront mises en place pour garantir la qualité, la sécurité et l'intégrité des données. Un catalogue de données détaillé sera développé et maintenu pour une traçabilité et une compréhension optimales des sources de données.

