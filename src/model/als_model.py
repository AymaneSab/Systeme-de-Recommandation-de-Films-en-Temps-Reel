        try :
            # Assuming calculate_user_average is defined correctly and available
            # calculate_user_average_udf = lambda user_id: calculate_user_average(user_id, session, user_logger)

            # treated_users = treated_users.withColumn('user_activity', lit(calculate_user_average_udf(col("userId").cast(StringType()))))


            user_logger.info("Aggregation Succefull")

        except Exception as e :
            user_logger.error(f"Aggregation Failed {str(e)}")




            def sparkTreatment_user(topicname, kafka_bootstrap_servers, spark_logger, user_logger):
    try:
        # Initialize Spark session
        session = SparkSession.builder.appName("YourAppName").getOrCreate()
        user_logger.info("----------> Packages Loaded Successfully ")

        # Define the schema for Kafka messages
        kafka_schema = StructType([
            StructField("userId", IntegerType(), True),
            StructField("age", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("occupation", StringType(), True),
            StructField("zipcode", StringType(), True)
        ])

        # Read data from Kafka topic with defined schema
        df = session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topicname) \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(explode(from_json("value", ArrayType(kafka_schema))).alias("data")) \
            .select("data.*")
        
        try :

            treated_users = clean_and_preprocess_user_data(df)
            user_logger.info("Transformation Succefull")

        except Exception as e :
            user_logger.error(f"Transformation Failed {str(e)}")

        checkpoint_location = "Elasticsearch/Checkpoint/Users"

        if not os.path.exists(checkpoint_location):
            os.makedirs(checkpoint_location)

        es = connectToelastic(user_logger)
        createUserIndex(es, user_logger)

        # Write to Elasticsearch
        query = treated_users.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_location) \
            .foreachBatch(lambda df, epoch_id: write_to_elasticsearch(df, epoch_id, session, user_logger)) \
            .start()

        query.awaitTermination()
        user_logger.info("treated_users Sent To Elastic ")

    except StreamingQueryException as sqe:
        user_logger.error(f"Streaming query exception: {str(sqe)}")
    except Exception as e:
        user_logger.error(f"An error occurred: {str(e)}")
    finally:
        session.stop()

def write_to_elasticsearch(df, epoch_id, session, logger):
    try:
        xx = lambda batch_df, batch_id: batch_df.select("userId").collect()

        user_logger.info(f" userId : {xx} ")

        calculate_user_average_udf = lambda user_id: calculate_user_average(user_id, session, user_logger)

        treated_users = df.withColumn('user_activity', lit(calculate_user_average_udf(df['userId'])))

        # Write the batch data to Elasticsearch
        treated_users.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "user/_doc") \
            .mode("append") \
            .save()

    except Exception as e:
        # Handle the exception (print or log the error message)
        logger.error(f"Error in write_to_elasticsearch: {str(e)}")