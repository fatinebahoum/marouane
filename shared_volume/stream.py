from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("KafkaCassandraStreaming").getOrCreate()

# Cassandra connection parameters
cassandra_host = "cassandra"
cassandra_port = "9042"
cassandra_keyspace = "bigdataproject"

# Define the schema for the data from the Bitcoin Kafka topic
bitcoin_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Symbol", StringType(), True),
    StructField("num_market_pairs", IntegerType(), True),
    StructField("max_supply", LongType(), True),
    StructField("infinite_supply", BooleanType(), True),
    StructField("last_updated", TimestampType(), True),
    StructField("Price_USD", DoubleType(), True),
    StructField("Market_Cap_USD", FloatType(), True),
    StructField("Volume_24h_USD", FloatType(), True),
    StructField("Percent_Change_24h", FloatType(), True),
    StructField("percent_change_1h", FloatType(), True)
])

# Define the schema for the data from the Reddit Kafka topic
reddit_schema = StructType([
    StructField("created_utc", TimestampType(), True),
    StructField("subreddit", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("title", StringType(), True),
    StructField("subreddit_name_prefixed", StringType(), True),
    StructField("upvote_ratio", FloatType(), True),
    StructField("category", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created", TimestampType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("view_count", IntegerType(), True),
    StructField("send_replies", BooleanType(), True)
])

# Create a Spark Streaming context with a batch interval of 5 seconds
ssc = StreamingContext(spark.sparkContext, 5)

# Kafka parameters
kafka_bootstrap_servers = "broker:9092"
kafka_topics = {"bitcoin_data": 1, "reddit_data": 1}  # Map of topic names to the number of Kafka partitions

# Create DStream from Kafka topics
kafka_stream = KafkaUtils.createStream(ssc, kafka_bootstrap_servers, "spark-streaming-consumer", kafka_topics)

# Parse JSON data from Kafka topics
parsed_stream_bitcoin = kafka_stream.map(lambda x: Row(**x[1]))
parsed_stream_reddit = kafka_stream.map(lambda x: Row(**x[1]))

# Convert RDDs of Rows to DataFrame for each topic
df_bitcoin = spark.createDataFrame(parsed_stream_bitcoin, schema=bitcoin_schema)
df_reddit = spark.createDataFrame(parsed_stream_reddit, schema=reddit_schema)

# Write data to Cassandra tables for each topic
df_bitcoin.write.format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("table", "bitcoin_data") \
    .option("keyspace", cassandra_keyspace) \
    .option("spark.cassandra.connection.host", cassandra_host) \
    .option("spark.cassandra.connection.port", cassandra_port) \
    .save()

df_reddit.write.format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("table", "reddit_data") \
    .option("keyspace", cassandra_keyspace) \
    .option("spark.cassandra.connection.host", cassandra_host) \
    .option("spark.cassandra.connection.port", cassandra_port) \
    .save()

# Start the streaming context
ssc.start()

# Wait for the streaming context to be stopped
ssc.awaitTermination()
