from flask import render_template, Blueprint, request

blueprint = Blueprint('pages', __name__)

import psycopg2
import pandas as pd
from werkzeug.urls import url_parse
from sqlalchemy import create_engine
engine = create_engine('postgres://gajpivqijkldsv:e71d7868249438e0b78e6cc37825dad10f322ef598118335a624165f9311720f@ec2-54-211-210-149.compute-1.amazonaws.com:5432/dc5355dnsr456p')

conn = psycopg2.connect(dbname='dc5355dnsr456p', user='gajpivqijkldsv', password='e71d7868249438e0b78e6cc37825dad10f322ef598118335a624165f9311720f',
                        host='ec2-54-211-210-149.compute-1.amazonaws.com', port='5432', sslmode='require')
cursor = conn.cursor()

################
#### routes ####
################


@blueprint.route('/')
def home():
    return render_template('pages/home.html')


@blueprint.route('/about')
def about():
    return render_template('pages/about.html')

# ##### Stocks SPORTS   Other
@blueprint.route('/stocks')
def stocks():
    return render_template('pages/stocks.html')

@blueprint.route('/sports')
def sports():
    return render_template('pages/sports.html')

@blueprint.route('/other')
def other():
    return render_template('pages/other.html')

@blueprint.route('/yesterdays_baseball_games')
def sports_baseball_yesterdays_games():
    yesterdays_games = pd.read_sql_query("SELECT home_team_name, away_team_name, home_total_score, away_total_score  FROM public.yesterdays_games", engine)
    return render_template('pages/sports_baseball_yesterdays_games.html',table= yesterdays_games.to_html())

@blueprint.route('/todays_baseball_games')
def sports_baseball_todays_games():
    todays_games = pd.read_sql_query("SELECT home_team_name, away_team_name, game_time  FROM public.todays_games", engine)
    print(todays_games)
    return render_template('pages/sports_baseball_todays_games.html', table= todays_games.to_html())

@blueprint.route('/baseball_social')
def sports_baseball_social_top_tweets():
    cursor.execute("SELECT twitter_block  FROM public.daily_top_ten limit 10")
    top_ten = cursor.fetchall()
    return render_template('pages/sports_baseball_social_top_tweets.html',top_ten_ = top_ten,
                                top_ten1 = top_ten[0],
                                top_ten2 = top_ten[1],
                                top_ten3 = top_ten[2],
                                top_ten4 = top_ten[3],
                                top_ten5 = top_ten[4],
                                top_ten6 = top_ten[5],
                                top_ten7 = top_ten[6],
                                top_ten8 = top_ten[7],
                                top_ten9 = top_ten[8],
                                top_ten10 = top_ten[9],)
