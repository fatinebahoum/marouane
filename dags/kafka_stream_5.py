from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import uuid
import praw
import json
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import requests
import logging
import subprocess
from uuid import uuid4


default_args = {
    'owner': 'me',
    'start_date': datetime(2024, 1, 17, 00, 20)
}

client_id='9v-k1UrgJdsxZWA68WaYvg'
client_secret='vdbrrAq5qlc5y3KRzO0krYn1tXlSfA'
user_agent='medel/2.0 by OkRelationship479'

def coinMarket_function():
    from kafka import KafkaProducer

    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
        'start': '1',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': '1f5a2103-63e6-4bb8-8ee1-5e3e874c8bc7',
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)

        bitcoin_data = next((crypto for crypto in data['data'] if crypto['symbol'] == 'BTC'), None)

        if bitcoin_data:
            data = {
                'id': str(uuid4()),
                'name': bitcoin_data['name'],
                'symbol': bitcoin_data['symbol'],
                'num_market_pairs': bitcoin_data['num_market_pairs'],
                'max_supply': bitcoin_data['max_supply'],
                'infinite_supply': bitcoin_data['infinite_supply'],
                'last_updated': bitcoin_data['last_updated'],
                'price_usd': bitcoin_data['quote']['USD']['price'],
                'market_cap_usd': bitcoin_data['quote']['USD']['market_cap'],
                'volume_24h_usd': bitcoin_data['quote']['USD']['volume_24h'],
                'percent_change_24h': bitcoin_data['quote']['USD']['percent_change_24h'],
                'percent_change_1h': bitcoin_data['quote']['USD']['percent_change_1h'],
            }

            producer = KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms = 5000)
            producer.send('bitcoin_data', json.dumps(data).encode('utf-8'))
            
            print(f"Bitcoin data send to bitcoin_data topic at {datetime.now()}")

        else:
            print("Bitcoin data not found in the response.")

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
        
        
        
def reddit_function():
    from kafka import KafkaProducer

    reddit = praw.Reddit(
        client_id= client_id,
        client_secret= client_secret,
        user_agent= user_agent
    )
    subreddits = ['Bitcoin', 'btc', 'CryptoCurrency', 'BitcoinIndia', 'BitcoinMining','BitcoinCA','CryptoMarkets','CryptoCurrencies']
    # Calculate the timestamp for 5 minutes ago
    one_minute_ago = datetime.utcnow() - timedelta(minutes=5)
    one_minute_ago_timestamp = int(one_minute_ago.timestamp())
    for subreddit_name in subreddits:
        print(" submission : ")
        subreddit = reddit.subreddit(subreddit_name)


        desired_attributes = [
        'created_utc', 'subreddit', 'selftext', 'title', 'subreddit_name_prefixed','_comments_by_id',
        'upvote_ratio', 'category', 'score', 'created', 'num_comments', 'url', 'view_count', 'send_replies'
        ]

        for submission in subreddit.new(limit=30):
            post_info = {}  
            '''
            if submission.created_utc >= one_minute_ago_timestamp:
                post_info = {key: submission.__dict__[key] for key in desired_attributes if key in submission.__dict__}
                post_info['created_utc'] = datetime.utcfromtimestamp(post_info['created_utc']).strftime('%Y-%m-%d %H:%M:%S')
            '''
            if submission.created_utc >= one_minute_ago_timestamp:
                print("@True")
                for key in desired_attributes:
                    if key == 'subreddit':
                        post_info['subreddit'] = submission.subreddit.display_name
                    else:
                        post_info[key] = submission.__dict__[key]
                post_info['created_utc'] = datetime.utcfromtimestamp(post_info['created_utc']).strftime('%Y-%m-%d %H:%M:%S')
                post_info['id'] = str(uuid.uuid4())
    
                producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
                producer.send('reddit_data', json.dumps(post_info).encode('utf-8'))
            
            else:
                print("#False")

	    
            
dag = DAG(
    'Kafka_Producer_5',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False
)

coinMarket = PythonOperator(
    task_id='coinMarket',
    python_callable=coinMarket_function,
    dag=dag
)

reddit = PythonOperator(
    task_id='Reddit',
    python_callable=reddit_function,
    dag=dag
)

empty = DummyOperator(
    task_id='empty',
    dag=dag
)

empty.set_upstream([coinMarket, reddit])
  