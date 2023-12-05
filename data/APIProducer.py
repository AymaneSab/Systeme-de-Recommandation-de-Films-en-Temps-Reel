import os 
from datetime import datetime 
import logging 
import requests
import json 

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

def setup_Customers_logging():
    return setup_logging("Log/API_Customers_LogFiles", "customers_logger")

def getCustomers(logger):
    try:
        while True:
            try:
                # Use the streaming endpoint to get movie data
                customer_data_endpoint = 'http://localhost:5002/movie_data'
                response = requests.get(customer_data_endpoint, stream=True)

                for line in response.iter_lines():
                    try:
                        if line:
                            customer_json = json.loads(line)
                            logger.info(customer_json)

                    except ValueError as ve:
                        # Log the error if the JSON parsing fails
                        logger.info(f"Error parsing JSON: {ve}")

                    except Exception as ex:
                        # Log other validation errors
                        logger.info(f"Error validating Kafka message: {ex}")

                # Close the response after processing all lines
                response.close()

            except Exception as e:
                logger.error("Error getting movie data: " + str(e))

    except Exception as e:
        logger.error("Error producing to Kafka: " + str(e))

customers_logger = setup_Customers_logging()

getCustomers(customers_logger)

