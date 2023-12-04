# Système de Recommandation de Films en Temps Réel

Ce projet a pour objectif de concevoir un système de recommandation de films en temps réel en utilisant les technologies Apache Spark, Elasticsearch, Kibana et Flask. L'ensemble du processus, du prétraitement des données à la visualisation en passant par la construction du modèle de recommandation, est couvert dans ce projet.

## Objectif du Projet

L'objectif principal de ce projet est de créer une solution robuste et évolutive pour offrir une expérience pratique dans le traitement de données, l'apprentissage automatique et la visualisation, tout en mettant en œuvre un système de recommandation de films en temps réel.

## Configuration de l'Environnement

### Installation des Technologies

Assurez-vous d'installer et de configurer correctement les technologies nécessaires, notamment Apache Spark, Elasticsearch, Kibana et Flask. Les instructions détaillées peuvent être trouvées dans la documentation respective de chaque technologie.

### Acquisition des Données

Téléchargez l'ensemble de données MovieLens à partir de la source officielle. Cet ensemble de données servira de base pour la création du modèle de recommandation.

## Prétraitement des Données avec Apache Spark

### Chargement et Nettoyage des Données

Utilisez Apache Spark pour charger l'ensemble de données MovieLens, nettoyer les données en gérant les valeurs manquantes et convertir les types de données au besoin.

### Ingénierie des Caractéristiques

Créez des caractéristiques pertinentes pour le système de recommandation, telles que la mesure de l'activité de l'utilisateur, les évaluations moyennes des films, et fusionnez ces données pour obtenir un DataFrame complet.

## Construction du Modèle de Recommandation

### Sélection du Modèle

Choisissez un modèle approprié pour les recommandations de films. Dans ce projet, l'utilisation du modèle ALS (Alternating Least Squares) est recommandée pour sa pertinence dans le domaine de la recommandation collaborative.

### Entraînement et Évaluation du Modèle

Entraînez le modèle en utilisant le jeu de données préparé et évaluez ses performances. L'objectif est d'obtenir un modèle de recommandation précis et fiable.

## Intégration des Données dans Elasticsearch

### Création d'Indices et Ingestion des Données

Définissez et créez des indices Elasticsearch pour stocker les données de films et d'utilisateurs. Transformez ensuite les données prétraitées et effectuez l'ingestion dans Elasticsearch pour permettre une recherche rapide et efficace.

## Visualisation avec Kibana

### Création de Tableaux de Bord

Utilisez Kibana pour créer des visualisations et des tableaux de bord basés sur les données stockées dans Elasticsearch. Explorez des métriques telles que les évaluations moyennes des films et les niveaux d'activité des utilisateurs.

### Interprétation des Données

Interprétez les données visualisées à l'aide de Kibana pour extraire des insights significatifs. Comprenez les tendances et les comportements des utilisateurs pour améliorer la qualité des recommandations.

## Développement de l'API Flask

### Création d'une API de Recommandation

Développez une API Flask permettant aux utilisateurs de soumettre le titre d'un film. Utilisez les informations pour identifier les utilisateurs associés et générer des recommandations en temps réel à l'aide du modèle ALS.

## Déploiement

### Hébergement
