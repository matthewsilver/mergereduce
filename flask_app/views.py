from flask import render_template, request
from flask_app import app, db_config
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import psycopg2
import requests
from bs4 import BeautifulSoup

user = 'postgres' #add your username here (same as previous postgreSQL)                      
host = db_config.host
port = db_config.port
dbname = db_config.dbname
password = db_config.password
con = None
con = psycopg2.connect(host=host, port=port, database = dbname, user = user, password=password)

@app.route('/')
@app.route('/index')
def index():
    category = request.args.get('category')
    if not category:
        category="Weather modification"
    limit = request.args.get('limit')
    if not limit:
        limit = 10
    sql_query = """
    with s as (select * from scores where category like '%{}%' order by random() limit {}) select * from s order by score desc;

    """.format(category, limit)
    query_results = pd.read_sql_query(sql_query,con)
    scores = []
    for i in range(0,len(query_results)):
        row = query_results.iloc[i]
        score_dict = {}
        for key, value in dict(row).items():
            if key=='category':
                score_dict[key] = value.split('cat:')[1][:-1]
            else:
                score_dict[key] = value
        score_dict['Title1'] = BeautifulSoup(requests.get(score_dict['id1']).text)('title')[0].get_text().split(' - Wikipedia')[0] 
        score_dict['Title2'] = BeautifulSoup(requests.get(score_dict['id2']).text)('title')[0].get_text().split(' - Wikipedia')[0] 
        scores.append(score_dict)
    return render_template('index.html', scores=scores)
@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/guide')
def guide():
    return render_template('api_guide.html')
