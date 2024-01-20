import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import uuid
from transformers import pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, FloatType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdataproject
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    # Define the schema for your tables (reddit_data, bitcoin_data, SpeedLayer)
    session.execute("""
        CREATE TABLE IF NOT EXISTS bigdataproject.reddit_data (
            id UUID PRIMARY KEY,
            created_utc timestamp,
            subreddit text,
            selftext text,
            title text,
            subreddit_name_prefixed text,
            upvote_ratio float,
            category text,
            score int,
            created timestamp,
            num_comments int,
            url text,
            view_count int,
            send_replies boolean
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS bigdataproject.bitcoin_data (
            id UUID PRIMARY KEY,
            Name text,
            Symbol text,
            num_market_pairs int,
            max_supply bigint,
            infinite_supply boolean,
            last_updated timestamp,
            Price_USD DOUBLE,
            Market_Cap_USD float,
            Volume_24h_USD float,
            Percent_Change_24h float,
            percent_change_1h float
        );
    """)

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

    print("Tables created successfully!")

def insert_data(session, topic, row, **kwargs):
    try:
        if topic == 'reddit_data':
            # Insert data into the reddit_data table
            session.execute("""
                INSERT INTO bigdataproject.reddit_data (id, created_utc, subreddit, selftext, title, subreddit_name_prefixed, upvote_ratio, category, score, created, num_comments, url, view_count, send_replies)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (uuid.uuid4(), row['created_utc'], row['subreddit'], row['selftext'], row['title'], row['subreddit_name_prefixed'],
                  row['upvote_ratio'], row['category'], row['score'], row['created'], row['num_comments'], row['url'], row['view_count'], row['send_replies']))

        elif topic == 'bitcoin_data':
            # Insert data into the bitcoin_data table
            session.execute("""
                INSERT INTO bigdataproject.bitcoin_data (id, Name, Symbol, num_market_pairs, max_supply, infinite_supply, last_updated, Price_USD, Market_Cap_USD, Volume_24h_USD, Percent_Change_24h, percent_change_1h)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (uuid.uuid4(), row['Name'], row['Symbol'], row['num_market_pairs'], row['max_supply'], row['infinite_supply'],
                  row['last_updated'], row['Price (USD)'], row['Market Cap (USD)'], row['Volume 24h (USD)'], row['Percent Change 24h'], row['percent_change_1h']))

        else:
            raise ValueError(f"Unsupported topic: {topic}")
        
        logging.info(f"Data inserted successfully into {topic} table.")
        
    except Exception as e:
        logging.error(f"Error inserting data into {topic} table: {e}")
    
def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn,topic):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:9092') \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
        raise

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
    
def insert_data_SpeedLayer(session, row):
    # Insert data into the SpeedLayer table
    session.execute("""
        INSERT INTO bigdataproject.speedlayer (timestamp, Bitcoin_Price, Number_of_Posts, Change_Percent, Avg_Sentiment_Score_Bullish, Avg_Sentiment_Score_Bearish, Avg_Sentiment_Score_Neutral)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (row['last_updated'], row['Price (USD)'], row['Number_of_Posts'], row['Change_Percent'],
          row['Avg_Sentiment_Score_Bullish'], row['Avg_Sentiment_Score_Bearish'], row['Avg_Sentiment_Score_Neutral']))
    print("row inserted into speedlayer table !!")

def calculate_sentiment(row):
    classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
    candidate_labels = ['Bearish', 'Bullish', 'Neutral']
    if row['selftext'] != None : 
        text = row['title'] + ' : ' + row['selftext']
    else : 
        text = row['title']

    result = classifier(text, candidate_labels, multi_label=True)
    
    return {
        'Sentiment_Score_Bullish': result['scores'][candidate_labels.index('Bullish')],
        'Sentiment_Score_Bearish': result['scores'][candidate_labels.index('Bearish')],
        'Sentiment_Score_Neutral': result['scores'][candidate_labels.index('Neutral')]
    }

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

def calculate_average_scores(sentiment_scores_list):
    # Calculate the average scores for each sentiment category
    total_bullish = 0
    total_bearish = 0
    total_neutral = 0
    
    for scores in sentiment_scores_list:
        total_bullish += scores['Sentiment_Score_Bullish']
        total_bearish += scores['Sentiment_Score_Bearish']
        total_neutral += scores['Sentiment_Score_Neutral']

    num_records = len(sentiment_scores_list)
    
    avg_bullish = total_bullish / num_records if num_records > 0 else 0.5
    avg_bearish = total_bearish / num_records if num_records > 0 else 0.5
    avg_neutral = total_neutral / num_records if num_records > 0 else 0.5
    
    return {
        'Avg_Sentiment_Score_Bullish': avg_bullish,
        'Avg_Sentiment_Score_Bearish': avg_bearish,
        'Avg_Sentiment_Score_Neutral': avg_neutral
    }

def calculate_percentage_change(session, current_price):
    # Query the last recorded Bitcoin price from the SpeedLayer table
    result = session.execute("SELECT Bitcoin_Price FROM bigdataproject.SpeedLayer ORDER BY timestamp DESC LIMIT 1")
    last_price = result[0]['Bitcoin_Price'] if result else None

    # Calculate percentage change
    if last_price is not None:
        percentage_change = ((current_price - last_price) / last_price) * 100
    else:
        # If no previous record, set percentage_change to 0 (arbitrary value)
        percentage_change = 0

    return percentage_change

def select_fields(df, topic):
    if topic == 'bitcoin_data':
        selected_df = df.select('last_updated', 'Price (USD)')
    elif topic == 'reddit_data':
        selected_df = df.select('selftext', 'title')
    else:
        raise ValueError(f"Unsupported topic: {topic}")

    return selected_df


def main():
    try:
        # Create a Spark session
        spark = create_spark_connection()

        # Connect to Kafka and read data for bitcoin_data
        bitcoin_kafka_df = connect_to_kafka(spark, 'bitcoin_data')

        # Connect to Kafka and read data for reddit_data
        reddit_kafka_df = connect_to_kafka(spark, 'reddit_data')

        # Create Cassandra session and keyspace
        cassandra_session = create_cassandra_connection()
        create_keyspace(cassandra_session)

        # Create tables if they don't exist
        create_table(cassandra_session)

        # Define the schema for the JSON data from Kafka for bitcoin_data
        bitcoin_schema = StructType([
            StructField("Name", StringType()),
            StructField("Symbol", StringType()),
            StructField("num_market_pairs", StringType()),
            StructField("max_supply", StringType()),
            StructField("infinite_supply", StringType()),
            StructField("last_updated", StringType()),
            StructField("Price (USD)", StringType()),
            StructField("Market Cap (USD)", StringType()),
            StructField("Volume 24h (USD)", StringType()),
            StructField("Percent Change 24h", StringType()),
            StructField("percent_change_1h", StringType())
        ])

        # Parse the JSON data from Kafka for bitcoin_data
        bitcoin_parsed_df = bitcoin_kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", bitcoin_schema).alias("data")).select("data.*")

        # Define the schema for the JSON data from Kafka for reddit_data
        reddit_schema = StructType([
            StructField("created_utc", StringType()),
            StructField("subreddit", StringType()),
            StructField("selftext", StringType()),
            StructField("title", StringType()),
            StructField("subreddit_name_prefixed", StringType()),
            StructField("_comments_by_id", StringType()),
            StructField("upvote_ratio", StringType()),
            StructField("category", StringType()),
            StructField("score", StringType()),
            StructField("created", StringType()),
            StructField("num_comments", StringType()),
            StructField("url", StringType()),
            StructField("view_count", StringType()),
            StructField("send_replies", StringType())
        ])

        # Parse the JSON data from Kafka for reddit_data
        reddit_parsed_df = reddit_kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", reddit_schema).alias("data")).select("data.*")

        # Select fields for bitcoin_data and reddit_data
        bitcoin_data_df = select_fields(bitcoin_parsed_df, 'bitcoin_data')
        reddit_data_df = select_fields(reddit_parsed_df, 'reddit_data')

        # Insert data into Cassandra tables
        print("bitcoin_parsed_df : ",bitcoin_parsed_df)
        bitcoin_stream_query = (bitcoin_parsed_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option('checkpointLocation', '/tmp/checkpoint/bitcoin_data') \
        .options(table="bitcoin_data", keyspace="bigdataproject") \
        .start())

        
        print("reddit_parsed_df : ",reddit_parsed_df)
        reddit_stream_query = (reddit_parsed_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option('checkpointLocation', '/tmp/checkpoint/reddit_data') \
        .options(table="reddit_data", keyspace="bigdataproject") \
        .start())

        print("# Calculate sentiment scores for reddit_data")
        calculate_sentiment_udf = udf(calculate_sentiment_udf_func, MapType(StringType(), FloatType()))
        reddit_data_df_with_sentiment = reddit_data_df.withColumn("sentiment_scores", calculate_sentiment_udf("title", "selftext"))

        # Select relevant columns from the DataFrame
        selected_columns = [ "sentiment_scores.Sentiment_Score_Bullish", "sentiment_scores.Sentiment_Score_Bearish",
                     "sentiment_scores.Sentiment_Score_Neutral"]

        reddit_data_df_selected = reddit_data_df_with_sentiment.select(*selected_columns)
        
        print("# Calculate percentage change in Bitcoin price")
        current_price = bitcoin_data_df.select("Price (USD)").orderBy("last_updated", ascending=False).limit(1).collect()[0][0]
        percentage_change = calculate_percentage_change(cassandra_session, current_price)

        print("# Calculate average sentiment scores")
        average_scores_query = reddit_data_df_selected.groupBy().avg()

        query = (average_scores_query.writeStream
        .outputMode("update")  # You may need to adjust the output mode based on your requirements
        .format("memory")  # You can replace "console" with your desired sink, e.g., "memory", "parquet", etc.
        .queryName("result_table")
        .start())

        print("awaitTermination ...")
        query.awaitTermination(timeout=60)

        print("select scores")
        average_scores = spark.sql("SELECT * FROM result_table")
        print ("average scores : ",average_scores )

        # Create a row for the SpeedLayer table
        speed_layer_row = {
            'last_updated': bitcoin_data_df.select("last_updated").orderBy("last_updated", ascending=False).limit(1).collect()[0][0],
            'Price (USD)': current_price,
            'Number_of_Posts': reddit_data_df.count(),
            'Change_Percent': percentage_change,
            'Avg_Sentiment_Score_Bullish': average_scores['Avg_Sentiment_Score_Bullish'],
            'Avg_Sentiment_Score_Bearish': average_scores['Avg_Sentiment_Score_Bearish'],
            'Avg_Sentiment_Score_Neutral': average_scores['Avg_Sentiment_Score_Neutral']
        }
        print("# The row for the SpeedLayer table : ", speed_layer_row)
        # Insert data into SpeedLayer table
        insert_data_SpeedLayer(cassandra_session, speed_layer_row)

        # Start the Spark Streaming query
        query = (reddit_parsed_df \
            .writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option('checkpointLocation', '/tmp/checkpoint') \
            .option('keyspace', 'bigdataproject')\
            .option('table', 'speedlayer')\
            .option("failOnDataLoss", "false")\
            .start())

        # Wait for the query to terminate
        print("Awaiting termination...")
        bitcoin_stream_query.awaitTermination()
        reddit_stream_query.awaitTermination()
        #query.awaitTermination()
        print("Streaming terminated.")

    except Exception as e:
        logging.error(f"An error occurred in the Spark Streaming job: {e}")
        # Handle the exception based on your requirements
    finally:
        print("Done !!")

if __name__ == "__main__":
    main()


