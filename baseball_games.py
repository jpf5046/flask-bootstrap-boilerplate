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

#########################################################
##               Connecto to database                  ##
#########################################################
engine = create_engine('postgres://gajpivqijkldsv:e71d7868249438e0b78e6cc37825dad10f322ef598118335a624165f9311720f@ec2-54-211-210-149.compute-1.amazonaws.com:5432/dc5355dnsr456p')

conn = psycopg2.connect(dbname='dc5355dnsr456p', user='gajpivqijkldsv', password='e71d7868249438e0b78e6cc37825dad10f322ef598118335a624165f9311720f',
                        host='ec2-54-211-210-149.compute-1.amazonaws.com', port='5432', sslmode='require')
cursor = conn.cursor()

# this pulls ALL games

import requests
import pandas as pd
url = "https://api-baseball.p.rapidapi.com/games"

# querystring = { "ga": "5"}
querystring = {"league":"1","season":"2020"}
# querystring = {"game": 60985}

headers = {
    'x-rapidapi-host': "api-baseball.p.rapidapi.com",
    'x-rapidapi-key': "c979c3eb73msh33dfb7086f67483p1d1b77jsn294f2f2aa6cc"
    }

response = requests.request("GET", url, headers=headers, params=querystring)
# response = requests.request("GET", url, headers=headers)
jsonobj = response.json()
# print(response.text)
id_list = []
date_list = []
time_list = []
timestamp_list = []
time_zone_list = []
week_list =[]
status_long_list = []
status_short_list = []
county_id_list = []
county_name_list = []
country_code_list = []
leadue_id_list = []
leaugue_name_list = []
league_type_list = []
league_logo_list = []
league_season_list = []
team_home_id_list =[]
team_home_name_list = []
team_home_logo_list = []
team_away_id_list = []
team_away_name_list = []
team_away_logo_list = []
score_home_hit_list =[]
score_home_error_list = []
score_home_inning_1_list = []
score_home_inning_2_list = []
score_home_inning_3_list = []
score_home_inning_4_list = []
score_home_inning_5_list = []
score_home_inning_6_list = []
score_home_inning_7_list = []
score_home_inning_8_list = []
score_home_inning_9_list = []
score_home_inning_extra_list = []
score_home_inning_total_list = []
score_away_inning_1_list = []
score_away_inning_2_list = []
score_away_inning_3_list = []
score_away_inning_4_list = []
score_away_inning_5_list = []
score_away_inning_6_list = []
score_away_inning_7_list = []
score_away_inning_8_list = []
score_away_inning_9_list = []
score_away_inning_extra_list = []
score_away_inning_total_list = []


for i in range(0, len(jsonobj['response'])):
    id_list.append(jsonobj['response'][i]['id'])
    date_list.append(jsonobj['response'][i]['date'])
    time_list.append(jsonobj['response'][i]['time'])
    timestamp_list.append(jsonobj['response'][i]['timestamp'])
    time_zone_list.append(jsonobj['response'][i]['timezone'])
    week_list.append(jsonobj['response'][i]['week'])
    status_long_list.append(jsonobj['response'][i]['status']['long'])
    status_short_list.append(jsonobj['response'][i]['status']['short'])
    county_id_list.append(jsonobj['response'][i]['country']['id'])
    county_name_list.append(jsonobj['response'][i]['country']['name'])
    country_code_list.append(jsonobj['response'][i]['country']['code'])
    leadue_id_list.append(jsonobj['response'][i]['league']['id'])
    leaugue_name_list.append(jsonobj['response'][i]['league']['name'])
    league_type_list.append(jsonobj['response'][i]['league']['type'])
    league_logo_list.append(jsonobj['response'][i]['league']['logo'])
    league_season_list.append(jsonobj['response'][i]['league']['season'])
    team_home_id_list.append(jsonobj['response'][i]['teams']['home']['id'])
    team_home_name_list.append(jsonobj['response'][i]['teams']['home']['name'])
    team_home_logo_list.append(jsonobj['response'][i]['teams']['home']['logo'])
    team_away_id_list.append(jsonobj['response'][i]['teams']['away']['id'])
    team_away_name_list.append(jsonobj['response'][i]['teams']['away']['name'])
    team_away_logo_list.append(jsonobj['response'][i]['teams']['away']['logo'])
    if jsonobj['response'][i]['scores']['home']['hits'] == None:
        score_home_hit_list.append('')
    else:
        score_home_hit_list.append(jsonobj['response'][i]['scores']['home']['hits'])
    if jsonobj['response'][i]['scores']['home']['errors'] == None:
        score_home_error_list.append('')
    else:
        score_home_error_list.append(jsonobj['response'][i]['scores']['home']['errors'])
    if jsonobj['response'][i]['scores']['home']['innings']['1'] == None:
        score_home_inning_1_list.append('')
    else:
        score_home_inning_1_list.append(jsonobj['response'][i]['scores']['home']['innings']['1'])
    if jsonobj['response'][i]['scores']['home']['innings']['2'] == None:
        score_home_inning_2_list.append('')
    else:
        score_home_inning_2_list.append(jsonobj['response'][i]['scores']['home']['innings']['2'])
    if jsonobj['response'][i]['scores']['home']['innings']['3'] == None:
        score_home_inning_3_list.append('')
    else:
        score_home_inning_3_list.append(jsonobj['response'][i]['scores']['home']['innings']['3'])
    if jsonobj['response'][i]['scores']['home']['innings']['4'] == None:
        score_home_inning_4_list.append('')
    else:
        score_home_inning_4_list.append(jsonobj['response'][i]['scores']['home']['innings']['4'])
    if jsonobj['response'][i]['scores']['home']['innings']['5'] == None:
        score_home_inning_5_list.append('')
    else:
        score_home_inning_5_list.append(jsonobj['response'][i]['scores']['home']['innings']['5'])
    if jsonobj['response'][i]['scores']['home']['innings']['6'] == None:
        score_home_inning_6_list.append('')
    else:
        score_home_inning_6_list.append(jsonobj['response'][i]['scores']['home']['innings']['6'])
    if jsonobj['response'][i]['scores']['home']['innings']['7'] == None:
        score_home_inning_7_list.append('')
    else:
        score_home_inning_7_list.append(jsonobj['response'][i]['scores']['home']['innings']['7'])
    if jsonobj['response'][i]['scores']['home']['innings']['8'] == None:
        score_home_inning_8_list.append('')
    else:
        score_home_inning_8_list.append(jsonobj['response'][i]['scores']['home']['innings']['8'])
    if jsonobj['response'][i]['scores']['home']['innings']['9'] == None:
        score_home_inning_9_list.append('')
    else:
        score_home_inning_9_list.append(jsonobj['response'][i]['scores']['home']['innings']['9'])
    if jsonobj['response'][i]['scores']['home']['innings']['extra'] == None:
        score_home_inning_extra_list.append('')
    else:
        score_home_inning_extra_list.append(jsonobj['response'][i]['scores']['home']['innings']['extra'])
    if jsonobj['response'][i]['scores']['home']['total'] == None:
        score_home_inning_total_list.append('')
    else:
        score_home_inning_total_list.append(jsonobj['response'][i]['scores']['home']['total'])
    if jsonobj['response'][i]['scores']['away']['innings']['1'] == None:
        score_away_inning_1_list.append('')
    else:
        score_away_inning_1_list.append(jsonobj['response'][i]['scores']['away']['innings']['1'])
    if jsonobj['response'][i]['scores']['away']['innings']['2'] == None:
        score_away_inning_2_list.append('')
    else:
        score_away_inning_2_list.append(jsonobj['response'][i]['scores']['away']['innings']['2'])
    if jsonobj['response'][i]['scores']['away']['innings']['3'] == None:
        score_away_inning_3_list.append('')
    else:
        score_away_inning_3_list.append(jsonobj['response'][i]['scores']['away']['innings']['3'])
    if jsonobj['response'][i]['scores']['away']['innings']['4'] == None:
        score_away_inning_4_list.append('')
    else:
        score_away_inning_4_list.append(jsonobj['response'][i]['scores']['away']['innings']['4'])
    if jsonobj['response'][i]['scores']['away']['innings']['5'] == None:
        score_away_inning_5_list.append('')
    else:
        score_away_inning_5_list.append(jsonobj['response'][i]['scores']['away']['innings']['5'])
    if jsonobj['response'][i]['scores']['away']['innings']['6'] == None:
        score_away_inning_6_list.append('')
    else:
        score_away_inning_6_list.append(jsonobj['response'][i]['scores']['away']['innings']['6'])
    if jsonobj['response'][i]['scores']['away']['innings']['7'] == None:
        score_away_inning_7_list.append('')
    else:
        score_away_inning_7_list.append(jsonobj['response'][i]['scores']['away']['innings']['7'])
    if jsonobj['response'][i]['scores']['away']['innings']['8'] == None:
        score_away_inning_8_list.append('')
    else:
        score_away_inning_8_list.append(jsonobj['response'][i]['scores']['away']['innings']['8'])
    if jsonobj['response'][i]['scores']['away']['innings']['9'] == None:
        score_away_inning_9_list.append('')
    else:
        score_away_inning_9_list.append(jsonobj['response'][i]['scores']['away']['innings']['9'])
    if jsonobj['response'][i]['scores']['away']['innings']['extra'] == None:
        score_away_inning_extra_list.append('')
    else:
        score_away_inning_extra_list.append(jsonobj['response'][i]['scores']['away']['innings']['extra'])
    if jsonobj['response'][i]['scores']['away']['total'] == None:
        score_away_inning_total_list.append('')
    else:
        score_away_inning_total_list.append(jsonobj['response'][i]['scores']['away']['total'])
   
   
list_of_games = pd.DataFrame({
                'game_id':                id_list,
                'game_date':               date_list,
                'game_time':               time_list,
                'game_timestap':                timestamp_list,
                'game_time_zone':                time_zone_list,
                'game_week':               week_list,
                'game_status_long':                status_long_list ,
                'game_status_short':                status_short_list,
                'game_county_id':                county_id_list,
                'game_county_name':                county_name_list,
                'game_county_code':                country_code_list,
                'league_id':                leadue_id_list,
                'league_name' :               leaugue_name_list,
                'league_type' :              league_type_list ,
                'league_logo' :              league_logo_list,
                'league_season':                league_season_list,
                'home_team_id' :             team_home_id_list,
                'home_team_name':               team_home_name_list,
                'home_team_logo' :               team_home_logo_list,
                'away_team_id':                team_away_id_list ,
                'away_team_name' :               team_away_name_list ,
                'away_team_logo' :               team_away_logo_list,
                'home_hits':                score_home_hit_list,
                'home_errors':                score_home_error_list,
                'home_inning_1_score' :               score_home_inning_1_list ,
                'home_inning_2_score' :               score_home_inning_2_list ,
                'home_inning_3_score' :               score_home_inning_3_list,
                'home_inning_4_score' :               score_home_inning_4_list,
                'home_inning_5_score'  :              score_home_inning_5_list ,
                'home_inning_6_score' :               score_home_inning_6_list ,
                'home_inning_7_score' :               score_home_inning_7_list ,
                'home_inning_8_score'  :              score_home_inning_8_list,
                'home_inning_9_score' :               score_home_inning_9_list ,
                'home_inning_ex_score' :               score_home_inning_extra_list,
                'home_total_score':                score_home_inning_total_list ,
                'away_inning_1_score' :               score_away_inning_1_list ,
                'away_inning_2_score':                 score_away_inning_2_list,
                'away_inning_3_score' :                score_away_inning_3_list ,
                'away_inning_4_score' :                score_away_inning_4_list ,
                'away_inning_5_score'  :               score_away_inning_5_list ,
                'away_inning_6_score'  :               score_away_inning_6_list ,
                'away_inning_7_score'  :               score_away_inning_7_list ,
                'away_inning_8_score'  :               score_away_inning_8_list ,
                'away_inning_9_score'  :               score_away_inning_9_list ,
                'away_inning_ex_score' :                score_away_inning_extra_list,
                'away_total_score'   :             score_away_inning_total_list,
})

list_of_games['game_date'] = pd.to_datetime(list_of_games['game_date'].str.strip(), format='%Y/%m/%d')
list_of_games['new_date'] = pd.to_datetime(list_of_games['game_date'], format = '%Y-%m-%d %H:%M:%S', errors ='coerce')
list_of_games['new_date'] = list_of_games['new_date'].dt.tz_convert('US/Eastern')


import datetime as dt
yesterday = dt.datetime.today() - dt.timedelta(days=1)
today = dt.datetime.today()
tomorrow = dt.datetime.today() + dt.timedelta(days=1)
two_days = dt.datetime.today() + dt.timedelta(days=2)
todays_games = list_of_games[(list_of_games['new_date'] >= "{}-{}-{}".format(today.year, today.month, today.day)) & (list_of_games['new_date'] <= "{}-{}-{}".format(tomorrow.year, tomorrow.month, tomorrow.day ))]

todays_games.to_sql('todays_games', engine, schema='public', if_exists='replace', chunksize=1000, method='multi', index=False)

yesterday = dt.datetime.today() - dt.timedelta(days=1)
yesterdays_games = list_of_games[(list_of_games['new_date'] >= "{}-{}-{}".format(yesterday.year, yesterday.month, yesterday.day)) & (list_of_games['new_date'] <= "{}-{}-{}".format(today.year, today.month, today.day ))]


yesterdays_games.to_sql('yesterdays_games', engine, schema='public', if_exists='replace', chunksize=1000, method='multi', index=False)
