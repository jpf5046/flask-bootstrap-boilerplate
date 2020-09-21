import pandas as pd
import requests
import numpy as np
import json
import csv
import time
import datetime
import urllib3
import sys
import os
import warnings
import pandas as pd
import os
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import warnings
from datetime import datetime
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)



engine = create_engine('postgres://gajpivqijkldsv:e71d7868249438e0b78e6cc37825dad10f322ef598118335a624165f9311720f@ec2-54-211-210-149.compute-1.amazonaws.com:5432/dc5355dnsr456p')

conn = psycopg2.connect(dbname='dc5355dnsr456p', user='gajpivqijkldsv', password='e71d7868249438e0b78e6cc37825dad10f322ef598118335a624165f9311720f',
                        host='ec2-54-211-210-149.compute-1.amazonaws.com', port='5432', sslmode='require')
cursor = conn.cursor()


most_recent = pd.read_csv("most_recent_tweets.csv")
## need to get all twitter handles
## might be easier to look for the twitter handles based on
import tweepy
import csv
import pandas as pd
import numpy as np
import time
import datetime

ACCESS_TOKEN = "924682665399382016-saFgc1u1ASXXhueEhoIhoLtC0h1PUQx"
ACCESS_TOKEN_SECRET = "TOTHNVVqqpswW401Cg2G7c6NzAgmvMjIiT2p1BsczBvYu"
CONSUMER_KEY = "KR0RquMlrK6RInnZvhL2ibhQF"
CONSUMER_SECRET = "aDt5c5M0PJP6EfipBDiH2rsVzx87vGaQQ630aAfMYc4ZEFZFve"

# OAuth process, using the keys and tokens
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# Creation of the actual interface, using authentication
api = tweepy.API(auth, wait_on_rate_limit=True)

handle = most_recent['handle'].to_list()
since_id = most_recent['status_id'].to_list()

user_name_list = []
fulltest_list = []
status_id_list = []
created_list = []
source_list = []
favorite_count = []
retweet_count = []
coordinates_list = []
geo_list = []
handle_list = []
follower_count_list = []
city_list = []
city_abr_list = []
league_list = []
division_list = []

for i in range(0,len(handle)):

    followercount = api.get_user('{}'.format(handle[i])).followers_count

    for status in tweepy.Cursor(api.user_timeline, screen_name= handle[i], tweet_mode="extended",since_id=since_id[i]).items():
#     for status in tweepy.Cursor(api.user_timeline, screen_name= mlbHandles[i], tweet_mode="extended").items():
        user_name_list.append(status.user.name)
        fulltest_list.append(status.full_text)
        status_id_list.append(np.array(status.id))
        created_list.append(status.created_at)
        source_list.append(status.source)
        favorite_count.append(status.favorite_count)
        retweet_count.append(status.retweet_count)
        coordinates_list.append(status.coordinates)
        geo_list.append(status.geo)
        handle_list.append(handle[i])
#         city_list.append(city_mlb[i])
#         city_abr_list.append(city_abr_mlb[i])
#         league_list.append(league_mlb[i])
#         division_list.append(division_mlb[i])
        follower_count_list.append(followercount)
    
    time.sleep(5)

newest_tweets = pd.DataFrame({
        'username': user_name_list,
        'full_text': fulltest_list,
        'status_id': status_id_list,
        'create_at': created_list,
        'source': source_list,
        'favorite_count': favorite_count,
        'retweet_count': retweet_count,
        'coordinates': coordinates_list,
        'geo': geo_list,
        'handle': handle_list,
        'followercount': follower_count_list,   
#         'city': city_list,
#         'city_abr':city_abr_list,
#         'league':league_list,
#         'disivion':division_list
        
})

print('finished pulling new tweets')

# save off latest tweet id
most_recent = newest_tweets[['handle', 'create_at', 'status_id']]
most_recent = most_recent[most_recent['create_at'] == most_recent.groupby('handle')['create_at'].transform('max')]
most_recent = most_recent.reset_index(drop=True)

most_recent.to_csv("most_recent_tweets.csv")

# code to create top ten tweets
now = pd.to_datetime('now')
from datetime import datetime, timedelta
d = now - timedelta(days=1)

#get ratio and volume
top_5_volume = newest_tweets[newest_tweets['create_at'] > d]
top_5_volume = top_5_volume.sort_values(by=['favorite_count'], ascending=False)
top_5_volume['favorite_ratio'] = top_5_volume['favorite_count']/top_5_volume['followercount']
top_5_ratio = top_5_volume.sort_values(by=['favorite_ratio'], ascending=False)
top_5_ratio = top_5_ratio.drop_duplicates(subset=['username'])
top_5_volume = top_5_volume.drop_duplicates(subset=['username'])
top_5_volume = top_5_volume.head(5)
top_5_list = top_5_volume['status_id'].to_list()

# make sure ratio tweets don't have tweets from top 5 
top_5_ratio = top_5_ratio[~top_5_ratio['status_id'].isin(top_5_list)]
top_5_ratio = top_5_ratio.drop_duplicates(subset = ['username'])
top_5_ratio = top_5_ratio.head(5)

# create one dataframe
tweets_top_ten = pd.concat([top_5_volume, top_5_ratio])
tweets_top_ten = tweets_top_ten.reset_index()

tweet_list = tweets_top_ten['status_id'].to_list()

# add twitter block
new_list_for_df = []
for i in range(0, len(tweet_list)):
    new_list_for_df.append(''' <blockquote class="twitter-tweet">
                        <a href="https://twitter.com/sdafsdafsd/status/{}?ref_src=twsrc%5Etfw"</a></blockquote> 
                        <script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script> '''.format(tweet_list[i]))
    
tweets_top_ten['twitter_block'] = new_list_for_df

tweets_top_ten['index'] = tweets_top_ten['index'].values.astype(int)
tweets_top_ten['favorite_count'] = tweets_top_ten['favorite_count'].values.astype(int)
tweets_top_ten['retweet_count'] = tweets_top_ten['retweet_count'].values.astype(int)

test = tweets_top_ten[['index', 'twitter_block', 'full_text', 'username', 'handle']]

# send to database
test.to_sql('daily_top_ten', engine, schema='public', if_exists='replace', chunksize=1000, method='multi', index=False)

print('finished tweets')
