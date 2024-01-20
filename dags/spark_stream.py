from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

# Create a Spark session
spark = SparkSession.builder.appName("SparkStreamProcessing").getOrCreate()

# Access parameters passed from Airflow
bitcoin_data_df_path = spark.conf.get("spark.submit.pyFiles")[0]
reddit_data_df_path = spark.conf.get("spark.submit.pyFiles")[1]

# Load Bitcoin data DataFrame
bitcoin_data_df = spark.read.parquet(bitcoin_data_df_path)

# Load Reddit data DataFrame
reddit_data_df = spark.read.parquet(reddit_data_df_path)

# Process data and calculate required metrics
processed_data_df = (
    # Assuming you have a DataFrame with appropriate columns
    # Replace the following code with your actual processing logic
    bitcoin_data_df
    .join(reddit_data_df, on="common_column", how="inner")
    .groupBy("last_updated")
    .agg(
        F.avg("Price (USD)").alias("Bitcoin_Price"),
        F.count("reddit_column").alias("Number_of_Posts"),
        # Add your additional aggregations here
    )
)

# Calculate additional metrics
processed_data_df = processed_data_df.withColumn(
    "Change_Percent",
    F.expr("(Bitcoin_Price - lag(Bitcoin_Price) over (order by last_updated)) / lag(Bitcoin_Price) over (order by last_updated) * 100")
)

# Assuming you have functions to calculate sentiment scores
# Replace these with your actual sentiment scoring logic
def calculate_sentiment_score_udf(title, selftext):
    # Your sentiment scoring logic here
    return 0.5

sentiment_score_udf = F.udf(calculate_sentiment_score_udf, FloatType())

processed_data_df = processed_data_df.withColumn(
    "Avg_Sentiment_Score_Bullish",
    sentiment_score_udf("title", "selftext")
)

# Repeat the above two blocks for other sentiment scores

# Write the processed data to Cassandra
processed_data_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="SpeedLayer", keyspace="BigDataProject") \
    .mode("append") \
    .save()
