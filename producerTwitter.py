import datetime
from kafka import KafkaProducer
import re
from textblob import TextBlob
import tweepy
from time import sleep

def kafka_python_producer_sync(producer, msg, topic):
    #print('Trying to send:' + msg)
    producer.send(topic, bytes(msg, encoding = 'utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)

def succes(metadata):
    print(metadata.topic)

def error(exeption):
    print(exeption)


def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(tweet)).split())

def analyze_sentiment(tweet):
    analysis = TextBlob(clean_tweet(tweet))
    if analysis.sentiment.polarity > 0:
        return 1
    elif analysis.sentiment.polarity == 0:
        return 0
    else:
        return -1

def twitter_sentiment():
    access_token = "490144630-6tTAhXAfMi8lZkeWKPX849WLkYDfdshWddYTKDE8"
    access_token_secret = "p6rWkG3nzb6yNrbBo56mNyaLORvXNdOSF6SR7Xuo58lgp"
    api_key = "lkPz7jCAjKFMU99aGowBGsVMn"
    api_key_secret = "LVFwih2A6iy0rLDpzHovi1FNvYEe8aVD1X5UGIVyqE7dQrSWhs"
    auth = tweepy.OAuthHandler(consumer_key=api_key, consumer_secret=api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    tweets = tweepy.Cursor(api.search_tweets, q = ['Bitcoin', 'BTC']).items(15)

    return tweets
    #
    # for tweet in tweets:
    #     cleaned = clean_tweet(tweet.text)
    #     sentiment = analyze_sentiment(cleaned)
    #     timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #
    #     return '{"Text":"%s", "Sentiment":%d, "Time":"%s"}' % (cleaned, sentiment, timestamp)

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers = '34.70.54.211:9092')

    i = 1

    while i == 1:

        data = twitter_sentiment()

        for tweet in data:
            cleaned = clean_tweet(tweet.text)
            sentiment = analyze_sentiment(cleaned)

            if sentiment == 1:
                senti_pos = 1
                senti_neu = 0
                senti_neg = 0
            elif sentiment == 0:
                senti_pos = 0
                senti_neu = 1
                senti_neg = 0
            else:
                senti_pos = 0
                senti_neu = 0
                senti_neg = 1

            timestamp = tweet.created_at.strftime('%Y-%m-%d %H:%M:%S')

            text = '{"Time":"%s", "Text":"%s", "Positive":%d, "Neutral":%d, "Negative":%d}' % (timestamp, cleaned, senti_pos, senti_neu, senti_neg)

            kafka_python_producer_sync(producer, text, 'twitter')
        sleep(10)
    f.close()