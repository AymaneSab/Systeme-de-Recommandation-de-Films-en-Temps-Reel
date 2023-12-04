# Système de Recommandation de Films en Temps Réel

Ce projet vise à créer un système de recommandation de films en temps réel en utilisant Apache Spark, Elasticsearch, Kibana et Flask pour offrir une expérience pratique dans le traitement de données, l'apprentissage automatique et la visualisation.

## Compréhension du Projet

### Objectif
Se familiariser avec le but - créer un système de recommandation de films en temps réel et visualiser les données.

### Ressources
Examiner l'ensemble de données MovieLens.

## Configuration de l'Environnement

### Installation
Installer et configurer Apache Spark, Elasticsearch, Kibana et Flask.

### Acquisition des Données
Télécharger l'ensemble de données MovieLens.

## Prétraitement des Données avec Apache Spark

### Chargement des Données
Charger l'ensemble de données MovieLens dans Spark.

### Nettoyage des Données
Nettoyer et prétraiter les données (gestion des valeurs manquantes, conversion des types de données, diviser le champ des genres en une liste pour une meilleure analyse dans Elasticsearch).

### Ingénierie des Caractéristiques
Créer les caractéristiques nécessaires pour le système de recommandation (mesure de l'activité de l'utilisateur, évaluations moyennes des films, fusionner les notes moyennes avec le DataFrame des films pour inclure les détails du film).

## Construction du Modèle de Recommandation

### Sélection du Modèle
Choisir un modèle approprié pour les recommandations de films (par exemple, ALS model).

### Entraînement du Modèle
Former le modèle en utilisant le jeu de données préparé.

### Évaluation du Modèle
Évaluer les performances du modèle.

## Intégration des Données dans Elasticsearch

### Création d'Indices
Définir et créer des indices Elasticsearch pour les données de films et d'utilisateurs.

### Transformation des Données
Transformer les données pour l'ingestion dans Elasticsearch.

### Ingestion des Données
Ingestion des données traitées dans Elasticsearch.

## Visualisation avec Kibana

### Création de Tableaux de Bord
Créer des visualisations et des tableaux de bord dans Kibana basés sur les données Elasticsearch (par exemple, évaluations moyennes des films, niveaux d'activité des utilisateurs).

### Interprétation des Données
Interpréter et expliquer les insights obtenus à partir des visualisations.

## Développement de l'API Flask

Développer une API qui reçoit le titre du film des utilisateurs. Étant donné que le modèle ALS nécessite un ID utilisateur pour générer des recommandations, vous devez rechercher les utilisateurs qui ont interagi avec le film spécifié, puis utiliser ces ID utilisateur pour obtenir des recommandations.

## Déploiement

Déployez l'application Flask sur un serveur Web ou une plateforme cloud. Vous pouvez utiliser des services tels que Heroku, ou une plate-forme similaire pour le déploiement.

## Conformité au RGPD et Gouvernance des Données

Assurer la conformité du système au RGPD, notamment en ce qui concerne le consentement des utilisateurs et la protection des données personnelles.

Mettre en place des politiques de gouvernance des données pour assurer la qualité, la sécurité et l'intégrité des données.

Développer et maintenir un catalogue de données détaillé pour une meilleure traçabilité et compréhension des sources de données.
