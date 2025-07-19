#API connections
import os
from airflow.hooks.base import BaseHook
import requests
from datetime import datetime, timedelta


api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
news_key = os.getenv("NEWS_API_KEY")

def fetch_stock_API():
    stock_api = BaseHook.get_connection('alpha_vantage_api')
    api_key = stock_api.extra_dejson.get("api_key")

    if not api_key:
        raise ValueError("Stock data API key is missing")
    
    #define variables
    symbol = "MP"
    function = "TIME_SERIES_DAILY"
    interval = "5min"

    #build url with the parameters
    url = f"https://www.alphavantage.co/query"
    parameters = {
        "function": function,
        "symbol": symbol,
        "interval": interval,
        "apikey": api_key
    }

    response = requests.get(url, params=parameters)

    stock_data = response.json()

    print(stock_data)

    return stock_data

def fetch_news_API():
    news_api = BaseHook.get_connection('news_api')
    api_news = news_api.extra_dejson.get("api_news")

    today = datetime.today().date()
    yesterday = today - timedelta(days=1)

    if not api_news:
        raise ValueError("News API key is missing")
    
    #define variables
    q = '"MP Materials" OR "rare earth"'


    #build url with the parameters
    url = f"https://newsapi.org/v2/everything"
    parameters = {
        "q": q,
        "from": str(yesterday),
        "to": str(today),   
        "apiKey": api_news,
        "language": "en",
        "pageSize": 100
    }

    response = requests.get(url, params=parameters)

    if response.status_code != 200:
        raise Exception(f"News API request failed with status {response.status_code}: {response.text}")

    news_data = response.json()

    print(news_data)

    return news_data