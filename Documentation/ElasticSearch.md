# Spark Streaming Movie Recommendation System

This project implements a real-time movie recommendation system using Apache Spark Streaming and Kafka. The system processes movie, review, and user data from Kafka topics, performs data cleaning and preprocessing, and stores the processed data in Elasticsearch for further analysis and recommendation.

## Prerequisites

- Apache Spark
- Apache Kafka
- Elasticsearch
- Python 3.x
- Required Python packages specified in `requirements.txt`

## Project Structure

- **`create_indices.py`**: Python script for creating Elasticsearch indices.
- **`clean_data.py`**: Python script for cleaning and preprocessing movie, review, and user data.
- **`feature_engineering.py`**: Python script for calculating user and movie averages.
- **`main_script.py`**: The main Spark Streaming script for processing and storing data.
- **`Log/`**: Directory to store log files.
- **`Elasticsearch/`**: Directory for storing Elasticsearch checkpoint data.

## Configuration

- Adjust the Kafka bootstrap servers and other configurations in the main script (`main_script.py`).
- Ensure the required packages are installed using `pip install -r requirements.txt`.

## Usage

1. Start the necessary services: Spark, Kafka, and Elasticsearch.
2. Run the main script: `python main_script.py`.
3. The script will create Spark Streaming jobs for processing movie, review, and user data concurrently.
4. Processed data will be stored in Elasticsearch indices.

## Logging

- Log files are stored in the `Log/` directory.
- Separate log files are created for Spark Streaming sessions, Elasticsearch requests, and each data type (Movies, Reviews, Users).

## Troubleshooting

- If an error occurs during execution, refer to the log files in the `Log/` directory for detailed information.

## Clean Up

- Stop the Spark Streaming jobs gracefully using `Ctrl+C`.
- Ensure to stop all related services (Spark, Kafka, Elasticsearch) when finished.

## Note

- This documentation assumes a basic understanding of Spark, Kafka, and Elasticsearch setup.

Feel free to customize the configuration and adapt the code according to your specific requirements.
