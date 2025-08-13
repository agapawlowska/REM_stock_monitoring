"""
This script uses VADER sentiment analysis.

Please cite:
Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for
Sentiment Analysis of Social Media Text. Eighth International Conference on
Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.

import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
nltk.download('all')



def get_data_from_postgres(table_name):
    conn = BaseHook.get_connection("pg_conn")
    uri = conn.get_uri().replace("postgres://", "postgresql://")
    engine = create_engine(uri)

    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    return df

def get_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()
    scores = analyzer.polarity_scores(text)
    sentiment = 1 if scores['pos'] > 0 else 0
    return sentiment
    """