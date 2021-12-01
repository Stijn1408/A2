from kafka import KafkaProducer
import requests
import pandas as pd
from time import sleep

# Function to collect price and percentage change in price for the ith most expensive coin at that moment
# Returns given batch number, timestamp of API call, symbol of ith most expensive coin, price and percentage change
# API is from CoinMarketCap, free key is obtained by registering on CoinMarketCap website
def get_btc_price_multiple(i, batch):
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

    apikey = '4b69f223-de6c-4013-8063-683be8703adc'

    headers = {
        'X-CMC_PRO_API_KEY': apikey,
        'Accepts': 'application/json'
    }

    params = {
        'start': i,
        'limit': i,
        'convert': 'USD'
    }

    json = requests.get(url, params=params, headers=headers).json()

    timestamp = json['status']['timestamp']
    timestamp = pd.to_datetime(timestamp, format='%Y-%m-%dT%H:%M:%S')
    timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

    coin = json['data'][0]
    symbol = coin['symbol']
    price = coin['quote']['USD']['price']
    percentChange = coin['quote']['USD']['percent_change_1h']

    output = '{"Batch": "%d", "Timestamp": "%s", "Symbol": "%s", "Price": "%f", "Percent_change_1h": "%f"}' % (batch, timestamp, symbol, price, percentChange)

    return output


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)

def success(metadata):
    print(metadata.topic)

def error(exception)
    print(exception)

def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()

# Main function with Kafka producer: sends five messages (of five most expensive cryptocurrency) per batch
# Sleeps 58 seconds after each batch, as sending five messages takes two seconds, the total cycle takes 60 seconds
# See report for future development: depending sleep time on time it takes to send five messages, to handle possible delays
if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='34.70.54.211:9092')  # Use your VM's external IP address here

    batch = 1

    i = 1

    while i == 1:
        for j in range(1, 6):
            data = get_btc_price_multiple(j, batch)
            kafka_python_producer_sync(producer, data, 'price')

        sleep(58)

        batch = batch + 1

    f.close()



