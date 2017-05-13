from __future__ import absolute_import, print_function

import os
import sys
import subprocess
import json
import pymysql
from datetime import datetime
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


def flatten(lst):
    t = []
    for i in lst:
        if not isinstance(i, list):
            t.append(i)
        else:
            t.extend(flatten(i))
    return t


with open('config.json', 'r') as f:
    config = json.load(f)

with open('rds_config.json', 'r') as f:
    rds_config = json.load(f)

#Variables that contains the user credentials to access Twitter API
access_token = config['access_token']
access_token_secret = config['access_token_secret']
consumer_key = config['consumer_key']
consumer_secret = config['consumer_secret']

#db connection details
hostname = rds_config['host']
dbname = rds_config['dbname']
username = rds_config['username']
passwd = rds_config['password']
port = rds_config['port']


def insert_tweet_data(data):
    print('inserting tweet data')
    if 'entities' in data:
        hashtags = [h['text'] for h in data['entities']['hashtags']]
        hashtags = json.dumps(hashtags)
    else:
        hashtags = ''

    if 'coordinates' in data and data['coordinates'] != None:
        lat, long = data['coordinates']['coordinates']
        coordinates = ','.join([str(lat), str(long)])
    else:
        coordinates = ''


    if 'place' in data and data['place'] != None:
        place = data['place']['full_name']
    else:
        place = ''

    conn = pymysql.connect(host=hostname,
                             user=username,
                             password=passwd,
                             port=port,
                             db=dbname,
                             charset='utf8')

    try:
        with conn.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `tweets` (`tweet_id`, `created_at`, `text`, `tweet_by_user_id`, `in_reply_to_user_id`, `coordinates`, `place`, `retweet_count`, `favorite_count`, `hashtags`) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (data['id_str'], data['created_at'], data['text'], data['user']['id'], data['in_reply_to_user_id'],
                                 data['coordinates'], place, data['retweet_count'], data['favorite_count'], hashtags))
            print("data inserted!")
        conn.commit()

    finally:
        conn.close()


def insert_user_data(data):
    print('inserting user data')

    conn = pymysql.connect(host=hostname,
                             user=username,
                             password=passwd,
                             port=port,
                             db=dbname,
                             charset='utf8')

    try:
        with conn.cursor() as cursor:
            # Create a new record
            sql = "INSERT IGNORE INTO `twitter_users` (`user_id`, `name`, `location`, `description`, `followers_count`, `friends_count`, `favourites_count`, `statuses_count`, `created_at`, `time_zone`, `lang`) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (data['user']['id_str'], data['user']['name'], data['user']['location'], data['user']['description'], data['user']['followers_count'],
                                 data['user']['friends_count'], data['user']['favourites_count'], data['user']['statuses_count'], data['user']['created_at'], data['user']['time_zone'], data['user']['lang']))
            print("data inserted!")
        conn.commit()

    finally:
        conn.close()


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    """

    def on_data(self, data):
        #add to file
        dt_now = datetime.now().date()
        f = open('data/tweet_data_' + str(dt_now) + '.txt', 'a')
        f.write(data)
        f.close()

        #add to mysqldb
        d = json.loads(data)
        if 'text' in d:
            insert_tweet_data(d)
        if 'user' in d:
            insert_user_data(d)
        return True


    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    tracking_words = ["#mother", "#mothersday", "happymothersday", "loveyoumom"]
    new_keywords = [x.strip() for x in sys.argv[1].split(',')]
    tracking_words.append(new_keywords)
    tracking_words = flatten(tracking_words)

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=tracking_words)
