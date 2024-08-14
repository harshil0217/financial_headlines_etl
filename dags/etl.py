import pandas as pd
import sqlalchemy
from airflow.decorators import dag, task
import pendulum
import feedparser

#engine to connect to postgres
#engine = sqlalchemy.create_engine('postgresql://airflow:airflow@postgres:5432')

feed = 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114'

@task()
def extract():
    feed = feedparser.parse(feed)
    for entry in feed.entries:
        print(entry.title)
        print(entry.link)
        print(entry.published)
        print(entry.summary)
        print('')
        
extract()