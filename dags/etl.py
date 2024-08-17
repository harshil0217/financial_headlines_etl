import pandas as pd
import sqlalchemy
from airflow.decorators import dag, task
import pendulum
import feedparser
import numpy as np
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch

#engine to connect to postgres
#engine = sqlalchemy.create_engine('postgresql://airflow:airflow@postgres:5432')

feed = 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114'


@task
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
    return 'headlines.csv'
    
@task
def classify(path):
    df = pd.read_csv(path)
    headlines = df['Title']
    tokenizer = AutoTokenizer.from_pretrained('bert-base-cased')
    model = AutoModelForSequenceClassification.from_pretrained('harshil0217/BERT_headline_classifier_v2')
    
    headlines = headlines.tolist()
    inputs = tokenizer(headlines, truncation=True, padding=True, return_tensors='pt')
    
    with torch.no_grad():
        output = model(**inputs)
        
    logits = output.logits
    
    probs = torch.nn.functional.softmax(logits, dim=1)
    preds = np.where(probs >=0.5, 1, 0)
    
    index_to_sentiment = {0: 'negative', 1: 'neutral', 2: 'positive'}
    sentiment = [index_to_sentiment[np.argmax(pred)] for pred in preds]
    
    df['Sentiment'] = sentiment
    df.to_csv('headlines.csv', index=False)
    
    return ('headlines.csv')
   
        
path = extract(feed)
path = classify(path)