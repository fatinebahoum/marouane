import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import uuid
from transformers import pipeline
from datetime import datetime, timedelta
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from cassandra.query import SimpleStatement
import joblib
import numpy as np
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler

def load_data_from_cassandra(spark, keyspace, table):
    # Chargez les données depuis Cassandra
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()
    return df

def predict_next_day(data, model, scaler):
    prediction = model.predict(data)
    prediction = scaler.inverse_transform(prediction)
    return prediction[0][0]

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataBatch') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdataproject
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS bigdataproject.batchlayer (
            timestamp TIMESTAMP PRIMARY KEY,
            open_price DOUBLE,
            close_price DOUBLE,
            posts_last_day INT,
            change_from_kafka DOUBLE,
            avg_sentiment_bullish DOUBLE,
            avg_sentiment_bearish DOUBLE,
            avg_sentiment_neutral DOUBLE,
            predicted_bitcoin_price DOUBLE,
            Max_Price DOUBLE,
            Min_Price DOUBLE
        );
    """)

    print("Table created successfully!")


def batch_processing():
    from cassandra.cluster import Cluster

    # Obtenez la date actuelle
    current_date = datetime.now()

    # Obtenez la date du début de la journée actuelle
    start_of_day = current_date.replace(hour=0, minute=0, second=0, microsecond=0)

    # Obtenez la date du début de la journée précédente
    start_of_previous_day = start_of_day - timedelta(days=1)
    five_days_ago = start_of_day - timedelta(days=5)

    # Formatez les dates pour les requêtes CQL
    current_date_str = current_date.strftime('%Y-%m-%d %H:%M:%S')
    start_of_day_str = start_of_day.strftime('%Y-%m-%d %H:%M:%S')
    start_of_previous_day_str = start_of_previous_day.strftime('%Y-%m-%d %H:%M:%S')
    five_days_ago_str = five_days_ago.strftime('%Y-%m-%d %H:%M:%S')
    # create spark connection
    spark_conn = create_spark_connection()

    # Connexion à Cassandra
    cluster = Cluster(['cassandra'])
    session = cluster.connect()

    create_keyspace(session)
    create_table(session)

    if(spark_conn):
        # Charger les données historiques depuis Cassandra
        print("Charger les données historiques depuis Cassandra")
        # Requête pour obtenir le bitcoin_price du premier timestamp du jour de la 1ere table
        print("start_of_day_str :", start_of_previous_day_str)
        query_first_timestamp = f"SELECT close_price FROM bigdataproject.batchlayer WHERE timestamp >= '{start_of_previous_day_str}' LIMIT 1 ALLOW FILTERING"

        # Requête pour obtenir le bitcoin_price du dernier timestamp du jour de la 1ere table
        query_last_timestamp = f"SELECT bitcoin_price FROM bigdataproject.speedlayer WHERE timestamp < '{current_date_str}' LIMIT 1 ALLOW FILTERING "

        # Requête pour obtenir la somme des posts_last_5_minutes du jour de la 1ere table
        query_sum_posts = f"SELECT SUM(posts_last_5_minutes) FROM bigdataproject.speedlayer WHERE timestamp >= '{start_of_previous_day_str}' AND timestamp < '{current_date_str}' ALLOW FILTERING"

        print("Exécutez les requêtes")
        first_timestamp_result = session.execute(query_first_timestamp).one()
        print("first_timestamp_result :", first_timestamp_result)
        last_timestamp_result = session.execute(query_last_timestamp).one()
        print("last_timestamp_result :", last_timestamp_result)
        sum_posts_result = session.execute(query_sum_posts).one()
        print("sum_posts_result :", sum_posts_result)

        # Obtenez les valeurs nécessaires
        print("Obtenez les valeurs nécessaires")
        first_timestamp_bitcoin_price = first_timestamp_result.close_price if first_timestamp_result else 0.0
        print("first_timestamp_bitcoin_price : ",first_timestamp_bitcoin_price)
        last_timestamp_bitcoin_price = last_timestamp_result.bitcoin_price if last_timestamp_result else 0.0
        print("last_timestamp_bitcoin_price : ",last_timestamp_bitcoin_price)
        sum_posts_last_day = sum_posts_result.system_sum_posts_last_5_minutes if sum_posts_result else 0
        print("sum_posts_last_day : ",sum_posts_last_day)

        # Calculs nécessaires
        close_price = last_timestamp_bitcoin_price
        open_price = first_timestamp_bitcoin_price
        if open_price != 0:
            change_from_kafka = ((close_price - open_price) / open_price) * 100
        else : 
            change_from_kafka = 0
        print("change_from_kafka :",change_from_kafka)
        # Requête pour obtenir la moyenne des avg_sentiment_bullish du jour de la 1ere table
        query_avg_sentiment_bullish = f"SELECT AVG(avg_sentiment_bullish) FROM bigdataproject.speedlayer WHERE timestamp >= '{start_of_previous_day_str}' AND timestamp < '{current_date_str}' ALLOW FILTERING"

        # Requête pour obtenir la moyenne des avg_sentiment_bearish du jour de la 1ere table
        query_avg_sentiment_bearish = f"SELECT AVG(avg_sentiment_bearish) FROM bigdataproject.speedlayer WHERE timestamp >= '{start_of_previous_day_str}' AND timestamp < '{current_date_str}' ALLOW FILTERING"

        # Requête pour obtenir la moyenne des avg_sentiment_neutral du jour de la 1ere table
        query_avg_sentiment_neutral = f"SELECT AVG(avg_sentiment_neutral) FROM bigdataproject.speedlayer WHERE timestamp >= '{start_of_previous_day_str}' AND timestamp < '{current_date_str}' ALLOW FILTERING"

        
        print("Exécutez les requêtes ...")
        avg_sentiment_bullish_result = session.execute(query_avg_sentiment_bullish).one()
        print("avg_sentiment_bullish_result : ",avg_sentiment_bullish_result)
        avg_sentiment_bearish_result = session.execute(query_avg_sentiment_bearish).one()
        print("avg_sentiment_bearish_result : ",avg_sentiment_bearish_result)
        avg_sentiment_neutral_result = session.execute(query_avg_sentiment_neutral).one()
        print("avg_sentiment_neutral_result : ",avg_sentiment_neutral_result)

        print("# Obtenez les valeurs moyennes nécessaires")
        avg_sentiment_bullish = avg_sentiment_bullish_result[0] if avg_sentiment_bullish_result else 0.5
        print("avg_sentiment_bullish : ",avg_sentiment_bullish)
        avg_sentiment_bearish = avg_sentiment_bearish_result[0] if avg_sentiment_bearish_result else 0.5
        print("avg_sentiment_bearish : ",avg_sentiment_bearish)
        avg_sentiment_neutral = avg_sentiment_neutral_result[0] if avg_sentiment_neutral_result else 0.5
        print("avg_sentiment_neutral : ",avg_sentiment_neutral)

        print("# Exécutez les requêtes...")
        max_price_result = session.execute(f"SELECT MAX(bitcoin_price) FROM bigdataproject.speedlayer WHERE timestamp >= '{start_of_previous_day_str}' AND timestamp < '{current_date_str}' ALLOW FILTERING").one()
        min_price_result = session.execute(f"SELECT MIN(bitcoin_price) FROM bigdataproject.speedlayer WHERE timestamp >= '{start_of_previous_day_str}' AND timestamp < '{current_date_str}' ALLOW FILTERING").one()

        max_price = max_price_result.system_max_bitcoin_price if max_price_result else 0.0
        min_price = min_price_result.system_min_bitcoin_price if min_price_result else 0.0
        print("max_price : ",max_price)
        print("min_price : ",min_price)

        model_path = '/opt/airflow/shared_volume/best_model_sen.h5'
        print("model_path : ",model_path)
        predicted_bitcoin_price = 0
        query_prices = f"SELECT timestamp, close_price, avg_sentiment_bullish, avg_sentiment_bearish, avg_sentiment_neutral FROM bigdataproject.batchlayer WHERE timestamp >= '{five_days_ago_str}' LIMIT 5 ALLOW FILTERING"
        prices_data = session.execute(query_prices).all()
        scaler = MinMaxScaler(feature_range=(0,1))
        scaler_path ='/opt/airflow/shared_volume/scaler.pkl'
        scaler = joblib.load(scaler_path)
        if(prices_data):
            print("load model ...")
            loaded_model = load_model(model_path)

            historical_data = [(row.avg_sentiment_bearish, row.avg_sentiment_bullish, row.avg_sentiment_neutral, row.close_price) for row in prices_data]
            bitcoin_prices = [row[-1] for row in historical_data]
            print("apply MinMaxScaler ..")
            scaled_bitcoin_prices = scaler.transform(np.array(bitcoin_prices).reshape(-1, 1))
            print("scaled_bitcoin_prices :",scaled_bitcoin_prices)
            input_data = np.hstack([np.array(historical_data)[:, 0:-1], scaled_bitcoin_prices])
            input_data = input_data.reshape(1, 5, 4)
            print("input data : ",input_data)
                # Faire la prédiction pour le prochain jour
            predicted_bitcoin_price = predict_next_day(input_data, loaded_model, scaler)
                
            predicted_bitcoin_price = str(predicted_bitcoin_price)
                
            print(f"Predicted Close Price for the Next Day: {predicted_bitcoin_price}")
        else: print("model path error")
        try:
                # Requête pour insérer les données dans la deuxième table
            insert_query = f"""
                INSERT INTO bigdataproject.batchlayer (
                    timestamp, open_price, close_price, posts_last_day, change_from_kafka,
                    avg_sentiment_bullish, avg_sentiment_bearish, avg_sentiment_neutral,
                    predicted_bitcoin_price, Max_Price, Min_Price
                ) VALUES (
                    '{current_date_str}', {open_price}, {close_price}, {sum_posts_last_day},
                    {change_from_kafka}, {avg_sentiment_bullish}, {avg_sentiment_bearish},
                    {avg_sentiment_neutral}, {predicted_bitcoin_price}, {max_price}, {min_price}
                )
            """
                # Exécutez la requête d'insertion
            session.execute(insert_query)
            logging.info(f"Inserted predicted value into Cassandra")

        except Exception as cassandra_error:
            logging.error(f'Error inserting predicted value into Cassandra: {cassandra_error}')
    else : 
        print("spark connection error")

    # Fermez la connexion
    cluster.shutdown()


if __name__ == "__main__":
    batch_processing()