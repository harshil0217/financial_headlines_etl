import pandas as pd
import sqlalchemy
from airflow.decorators import dag, task
import pendulum
import feedparser
import numpy as np
from dotenv import load_dotenv
import os
from google.cloud import language_v1


feed = 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114'

load_dotenv()


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
    headlines.to_csv('headlines.csv', index=False)
    return headlines
    

#classify headlines Title column in csv using google nlp
def classify(headlines):
    client = language_v1.LanguageServiceClient()
    
    categories = [None] * len(headlines)
    
    for i in range(len(headlines)):
        document = language_v1.Document(content = headlines['Title'][i], type_ = language_v1.Document.Type.PLAIN_TEXT)
        response = client.analyze_sentiment(request = {'document': document})
        sentiment = response.document_sentiment.score
        if sentiment > 0.66:
            category = 'positive'
        elif sentiment < -0.66:
            category = 'negative'
        else:
            category = 'neutral'
        
        categories[i] = category
        
    headlines['Sentiment'] = categories
        
    return headlines
        
    
    
        
    
    

@task
def load(headlines):
    #load data into database
    engine = sqlalchemy.create_engine(os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'))
    headlines.to_sql('headlines', con = engine, if_exists='replace', index=False)
    
    #test to see if working
    with engine.connect() as connection:
        query = "SELECT * FROM headlines LIMIT 3"
        print(connection.execute(query).fetchall())
        
    

@dag(
    dag_id='etl_pipeline',
    start_date=pendulum.datetime(2024, 8, 17, tz='EST'),
    schedule_interval='@daily',
    catchup=False
)
def pipeline():
    headlines = extract(feed)
    headlines = classify(headlines)
    load(headlines)
    

etl_pipeline = pipeline()

        
