from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define the default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 17, 00, 00),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Create a DAG object with the specified default_args
dag = DAG(
    'Stream_reddit_to_cassandra',
    default_args=default_args,
    description='DAG for streaming reddit data with Spark',
    schedule_interval=timedelta(minutes=5), 
    catchup=False,
)

spark_packages = "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

# Define the SparkSubmitOperator to run the Spark job
submit_spark_job = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='/opt/airflow/shared_volume/stream_reddit.py', 
    conn_id='spark_default',
    packages=spark_packages,
    conf={
        'spark.jars.packages': 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2',
        'spark.jars.excludes': 'org.scala-lang:scala-reflect',
    },
    dag=dag,
)

# Set the task dependencies
submit_spark_job