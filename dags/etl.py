import pandas as pd
import sqlalchemy
from airflow.decorators import dag, task
import pendulum
import feedparser
import numpy as np

#engine to connect to postgres
#engine = sqlalchemy.create_engine('postgresql://airflow:airflow@postgres:5432')

feed = 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114'


def extract(feed):
    feed = feedparser.parse(feed)
    n = len(feed.entries)
    #create empty dataframe
    headlines = pd.DataFrame(np.nan, index = range(n), columns = ['Title', 'Link', 'Published'])
    #iteratively fill headlines dataframe
    for i in range(n):
        headlines['Title'][i] = feed.entries[i].title
        headlines['Link'][i] = feed.entries[i].link
        headlines['Published'] = feed.entries[i].published
    #export to csv
    headlines.to_csv('headlines.csv')
    
        
extract(feed)