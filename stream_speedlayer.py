import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import uuid
from transformers import pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, FloatType
from datetime import datetime, timedelta

#create tables
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdataproject
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS bigdataproject.SpeedLayer (
            timestamp TIMESTAMP PRIMARY KEY,
            bitcoin_price DOUBLE,
            posts_last_5_minutes INT,
            change_from_kafka DOUBLE,
            avg_sentiment_bullish DOUBLE,
            avg_sentiment_bearish DOUBLE,
            avg_sentiment_neutral DOUBLE
        );
    """)

    print("Table created successfully!")


#connect to spark
def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkStreamingProcess') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

#connect to cassandra
def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

#read data
def read_last_row_from_bitcoin_data(spark):
    #Read the last row from the bitcoin_data table
    bitcoin_data = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="bitcoin_data", keyspace="bigdataproject") \
        .load()

    last_row = bitcoin_data.orderBy("last_updated", ascending=False).limit(1)
    return last_row.select("Price_USD", "last_updated")

def read_last_rows_from_reddit_data(spark):
    #Read the last rows inserted 5 minutes ago from the reddit_data table
    reddit_data = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="reddit_data", keyspace="bigdataproject") \
        .load()

    current_time = datetime.now()
    five_minutes_ago = current_time - timedelta(minutes=6)

    last_rows = reddit_data.filter(reddit_data.created_utc >= five_minutes_ago) \
        .orderBy("created_utc", ascending=False) \
        .limit(1)

    count_rows = reddit_data.filter(reddit_data.created_utc >= five_minutes_ago).count()

    return last_rows.select("selftext", "title"), count_rows

#calculate change percentage
def calculate_percentage_change(spark, current_price):
    #Read the last row from the SpeedLayer table

    print("conn")
    speed_layer = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="speedlayer", keyspace="bigdataproject") \
        .load()

    last_row = speed_layer.orderBy("timestamp", ascending=False).limit(1)
    if last_row.count() > 0:
        last_Price = last_row.select("bitcoin_price").collect()[0]['bitcoin_price']

        print(last_Price)

        # Calculate percentage change
        if(last_Price):
            percentage_change = ((current_price - last_Price) / last_Price) * 100
        else:
            # If no previous record, set percentage_change to 0 (arbitrary value)
            percentage_change = 0

        return percentage_change
    else:
        # Handle the case where there are no rows in the "speedlayer" table
        print("No previous record found in the 'speedlayer' table.")
        return 0

#Calculate sentiment
def calculate_sentiment_udf_func(title, selftext):
    classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
    candidate_labels = ['Bearish', 'Bullish', 'Neutral']
    
    if selftext is not None:
        text = title + ' : ' + selftext
    else:
        text = title
    
    result = classifier(text, candidate_labels, multi_label=True)
    
    return {
        'Sentiment_Score_Bullish': result['scores'][candidate_labels.index('Bullish')],
        'Sentiment_Score_Bearish': result['scores'][candidate_labels.index('Bearish')],
        'Sentiment_Score_Neutral': result['scores'][candidate_labels.index('Neutral')]
    }


#Insert data into speedLayer Table
def insert_data_SpeedLayer(session, row):
    # Insert data into the SpeedLayer table
    session.execute("""
        INSERT INTO bigdataproject.speedlayer (timestamp, bitcoin_price, posts_last_5_minutes, change_from_kafka, avg_sentiment_bullish, avg_sentiment_bearish, avg_sentiment_neutral)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (row['last_updated'], row['Price (USD)'], row['Number_of_Posts'], row['Change_Percent'],
          row['Avg_Sentiment_Score_Bullish'], row['Avg_Sentiment_Score_Bearish'], row['Avg_Sentiment_Score_Neutral']))
    print("row inserted into speedlayer table !!")


def processing():
    # create spark connection
    spark_conn = create_spark_connection()

    if(spark_conn):

        # Create Cassandra session and keyspace
        cassandra_session = create_cassandra_connection()

        if(cassandra_session):
            # Create keyspace if it don't exist
            create_keyspace(cassandra_session)

            # Create tables if they don't exist
            create_table(cassandra_session)

            #read data from bitcoin_data
            Price_USD, last_updated = read_last_row_from_bitcoin_data(spark_conn)

            #read data from reddit_data
            reddit_5min_data, count_rows= read_last_rows_from_reddit_data(spark_conn)

            #Calculate change percentage
            print("Calculate percentage change in Bitcoin price")
            percentage_change = calculate_percentage_change(spark_conn, Price_USD)

            print(percentage_change)
            

            #Calculate sentiment scores
            print("Calculate sentiment scores for reddit_data")
            # Define the UDF for sentiment calculation
            calculate_sentiment_udf = udf(calculate_sentiment_udf_func, MapType(StringType(), FloatType()))
            # Apply the sentiment UDF to each row
            reddit_data_with_sentiment = reddit_5min_data.withColumn("sentiment_scores", calculate_sentiment_udf("title", "selftext"))

            # Show the DataFrame with sentiment scores (for testing)
            reddit_data_with_sentiment.show(truncate=False)

            # Calculate average sentiment scores
            print("Calculate AVG sentiment scores for reddit_data")
            # Calculate average sentiment scores directly in Spark DataFrame
            avg_sentiment_scores = reddit_data_with_sentiment.selectExpr(
                "avg(sentiment_scores['Sentiment_Score_Bullish']) as Avg_Sentiment_Score_Bullish",
                "avg(sentiment_scores['Sentiment_Score_Bearish']) as Avg_Sentiment_Score_Bearish",
                "avg(sentiment_scores['Sentiment_Score_Neutral']) as Avg_Sentiment_Score_Neutral"
            ).collect()[0].asDict()


            # Create a row for the SpeedLayer table
            speed_layer_row = {
                'last_updated': last_updated,
                'Price (USD)': Price_USD,
                'Number_of_Posts': count_rows,
                'Change_Percent': percentage_change,
                'Avg_Sentiment_Score_Bullish': avg_sentiment_scores['Avg_Sentiment_Score_Bullish'],
                'Avg_Sentiment_Score_Bearish': avg_sentiment_scores['Avg_Sentiment_Score_Bearish'],
                'Avg_Sentiment_Score_Neutral': avg_sentiment_scores['Avg_Sentiment_Score_Neutral']
            }

            #insert data
            try:
                insert_data_SpeedLayer(cassandra_session, speed_layer_row)
            except Exception as cassandra_error:
                logging.error(f'Error inserting predicted value into Cassandra: {cassandra_error}')


if __name__ == "__main__":
    processing()