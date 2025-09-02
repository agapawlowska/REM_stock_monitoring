"""
This script uses VADER sentiment analysis.

Please cite:
Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for
Sentiment Analysis of Social Media Text. Eighth International Conference on
Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.
"""
import pandas as pd
from pandas.tseries.offsets import BDay
import nltk
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from bs4 import BeautifulSoup
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

from nltk.corpus import stopwords
stop_words = set(stopwords.words("english"))
wordnet_lemmatizer = WordNetLemmatizer()
sia = SentimentIntensityAnalyzer()


def preprocess_text(text):
    text = BeautifulSoup(text, "html.parser").get_text()
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)
    text = re.sub(r"\S+@\S+", "", text)
    text = re.sub(r"[^\w\s,.!?]", "", text)
    text = word_tokenize(text)
    text_no_stopwords = [word for word in text if word.lower() not in stop_words]
    lemmed_text = [wordnet_lemmatizer.lemmatize(word) for word in text_no_stopwords]
    return " ".join(lemmed_text)

def get_data_from_postgres(table_name):
    conn = BaseHook.get_connection("pg_conn")
    uri = conn.get_uri().replace("postgres://", "postgresql://")
    engine = create_engine(uri)

    query = f'SELECT * FROM "{table_name}"'
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def shift_news_date(date):
    weekday = date.weekday()

    if weekday == 6:  # Saturday
        return date + pd.Timedelta(days=1)
    elif weekday == 5:  # Friday
        return date + pd.Timedelta(days=2)
    else:
        return date + pd.Timedelta(days=1)

def get_sentiment(df):
    df['cleaned'] = df['content'].apply(preprocess_text)
    df['sentiment'] = df['cleaned'].apply(lambda x: sia.polarity_scores(x)['compound'])
    df['sentiment_label'] = df['sentiment'].apply(
        lambda x: 'positive' if x > 0.05 else ('negative' if x < -0.05 else 'neutral')
        )
    df['date_only'] = pd.to_datetime(df['publishedAt']).dt.date
    df['date_only_shifted'] = df['date_only'].apply(shift_news_date)
    df_news_agg = df.groupby('date_only_shifted').agg({
        'sentiment': 'mean',                         
        'sentiment_label': lambda x: x.mode()[0]     # if two the same entries - take the first one
        }).reset_index()
    
    return df_news_agg
