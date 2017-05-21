import util as Util
import json
import numpy as np
import pandas as pd
from datetime import datetime


def get_num_unique_users_by_hashtag():
    #Number unique users by #
    get_hashtags_sql_str = "select tweet_by_user_id, hashtags from tweets"
    hashtags_df = Util.extract_data_into_df(get_hashtags_sql_str)
    tracking_hashtags = ["hashtag1", "hashtag2"]
    hashtags_user_usage = dict(zip(tracking_hashtags, [0, 0, 0, 0, 0, 0]))

    uniq_users = np.unique(hashtags_df['tweet_by_user_id'])

    for u in uniq_users:
        user_tweets = hashtags_df[hashtags_df['tweet_by_user_id'] == u]
        for idx, row in user_tweets.iterrows():
            if Util.is_json(row['hashtags']):
                tweet_htags = json.loads(row['hashtags'])
                tr_htags_present = []
                for s in tweet_htags:
                    if Util.is_ascii(s):
                        if s in tracking_hashtags:
                            tr_htags_present.append(s)

                for t in np.unique(tr_htags_present):
                    hashtags_user_usage[t] += 1
    return json.dumps(hashtags_user_usage)


def get_num_unique_users_by_location():
    #Number unique users by location
    sql_str = "select count(user_id), location from twitter_users group by location order by count(user_id) desc"
    res = Util.extract_data_into_df(sql_str)
    num_users_by_loc = json.dumps(dict(zip(list(res['location'].values), list(res['count(user_id)'].values))))
    return num_users_by_loc


def get_num_unique_tweets_by_hashtag():
    #Number tweets by #
    get_hashtags_sql_str = "select tweet_by_user_id, hashtags from tweets"
    hashtags_df = Util.extract_data_into_df(get_hashtags_sql_str)
    tracking_hashtags = ["hashtag1", "hashtag2"]
    hashtags_tweet_usage = dict(zip(tracking_hashtags, [0, 0, 0, 0, 0, 0]))

    for idx, row in hashtags_df.iterrows():
        if Util.is_json(row['hashtags']):
            tweet_htags = json.loads(row['hashtags'])
            tr_htags_present = []
            for s in tweet_htags:
                if Util.is_ascii(s):
                    if s in tracking_hashtags:
                        tr_htags_present.append(s)

            for t in tr_htags_present:
                hashtags_tweet_usage[t] += 1
    return json.dumps(hashtags_tweet_usage)


def get_num_unique_tweets_by_location():
    #Number tweets by location
    sql_str = "select count(tweet_id), place from tweets group by place order by count(tweet_id) desc;"
    res = Util.extract_data_into_df(sql_str)
    num_tweets_by_loc = json.dumps(dict(zip(list(res['place'].values), list(res['count(tweet_id)'].values))))
    return num_tweets_by_loc


def get_top_n_tweets_by_hashtag(n):
    #Top N tweets by #
    get_hashtags_sql_str = "select hashtags, text from tweets"
    hashtags_df = Util.extract_data_into_df(get_hashtags_sql_str)
    tracking_hashtags = ['hashtag1', 'hashtag2']
    hashtags_tweet_text = {}

    for idx, row in hashtags_df.iterrows():
        if Util.is_json(row['hashtags']):
            tweet_htags = json.loads(row['hashtags'])
            tr_htags_present = []
            for s in tweet_htags:
                if Util.is_ascii(s):
                    if s in tracking_hashtags:
                        tr_htags_present.append([s, row['text']])

            for t in tr_htags_present:
                if str(t[0]) in hashtags_tweet_text:
                    if len(hashtags_tweet_text[str(t[0])]) <= n:
                        hashtags_tweet_text[str(t[0])].append(t[1])
                else:
                    hashtags_tweet_text[str(t[0])] = [t[1]]

    return json.dumps(hashtags_tweet_text)


def get_rank_of_most_popular_hashtags():
    #Rank of most popular hashtags
    num_tweets_by_hashtag = json.loads(get_num_unique_tweets_by_hashtag())
    hashtags_desc_order_of_numtweets = sorted(num_tweets_by_hashtag, key=num_tweets_by_hashtag.get, reverse=True)
    hashtag_ranks = dict(zip(hashtags_desc_order_of_numtweets, [1, 2, 3, 4, 5, 6]))
    return json.dumps(hashtag_ranks)


def get_summary_stats():
    print("Getting summary stats...")
    num_uniq_users_by_hashtag = get_num_unique_users_by_hashtag()
    num_uniq_users_by_location = get_num_unique_users_by_location()
    n = 5
    top_n_tweets_by_hashtag = get_top_n_tweets_by_hashtag(n)
    num_uniq_tweets_by_hashtag = get_num_unique_tweets_by_hashtag()
    num_uniq_tweets_by_location = get_num_unique_tweets_by_location()
    rank_of_most_popular_hashtags = get_rank_of_most_popular_hashtags()


def write_summary_stats_to_files():
    dt_now = datetime.now().date()

    fname_num_uniq_users_by_hashtag = 'results/summary_stats/num_uniq_users_by_hashtag_' + str(dt_now) + '.json'
    num_uniq_users_by_hashtag = get_num_unique_users_by_hashtag()
    Util.write_file(fname_num_uniq_users_by_hashtag, num_uniq_users_by_hashtag)

    fname_num_uniq_users_by_location = 'results/summary_stats/num_uniq_users_by_location_' + str(dt_now) + '.json'
    num_uniq_users_by_location = get_num_unique_users_by_location()
    Util.write_file(fname_num_uniq_users_by_location, num_uniq_users_by_location)

    fname_top_n_tweets_by_hashtag = 'results/summary_stats/top_n_tweets_by_hashtag_' + str(dt_now) + '.json'
    n = 5
    top_n_tweets_by_hashtag = get_top_n_tweets_by_hashtag(n)
    Util.write_file(fname_top_n_tweets_by_hashtag, top_n_tweets_by_hashtag)

    fname_num_uniq_tweets_by_hashtag = 'results/summary_stats/num_uniq_tweets_by_hashtag_' + str(dt_now) + '.json'
    num_uniq_tweets_by_hashtag = get_num_unique_tweets_by_hashtag()
    Util.write_file(fname_num_uniq_tweets_by_hashtag, num_uniq_tweets_by_hashtag)

    fname_num_uniq_tweets_by_location = 'results/summary_stats/num_uniq_tweets_by_location_' + str(dt_now) + '.json'
    num_uniq_tweets_by_location = get_num_unique_tweets_by_location()
    Util.write_file(fname_num_uniq_tweets_by_location, num_uniq_tweets_by_location)

    fname_rank_of_most_popular_hashtags = 'results/summary_stats/rank_of_most_popular_hashtags_' + str(dt_now) + '.json'
    rank_of_most_popular_hashtags = get_rank_of_most_popular_hashtags()
    Util.write_file(fname_rank_of_most_popular_hashtags, rank_of_most_popular_hashtags)


if __name__ == '__main__':
    write_summary_stats_to_files()
