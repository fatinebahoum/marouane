import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import uuid

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdataproject
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
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

    print("Tables created successfully!")


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkBitcoinDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:9092') \
            .option('subscribe', 'bitcoin_data') \
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

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
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

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()
    

    if spark_conn is not None:

        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        print(spark_df)

        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            

            logging.info("Streaming is being started...")
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint/btc')
                               .option('keyspace', 'bigdataproject')
                               .option('table', 'bitcoin_data')
                               .option("failOnDataLoss", "false")
                               .start())

            try:
                print("Awaiting termination...")
                streaming_query.awaitTermination(timeout=60000)
                print("Streaming terminated.")

            except Exception as e:
                logging.error(f"An error occurred while waiting for termination: {e}")

            finally:
                # Arrêter le streaming de manière explicite
                streaming_query.stop()
                print("Streaming arrêté manuellement.")