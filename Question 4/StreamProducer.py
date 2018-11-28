from kafka import KafkaProducer
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

consumer_key = "6QK1KsDmsMtsulwXb6nGeNMMZ"
consumer_secret = "KVcjL5OCvSas5Jq9vvw4uax9MnZrVqvVxfgZR24Lk7NWnN6xb3"
access_token = "1060250821076680704-cWMpj0gcguBsf1t32PrQueXuUujGB3"
access_secret = "zy6O4WW68VMlgxYXOd2IXkiJePQ6ecfoP5J8KrMdz5liE"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])



    def on_data(self, data):
        # Producer produces data for consumer

        self.producer.send("twitter", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has Game of Thrones hashtag (Tweets)
twitter_stream.filter(track=['#trump'])