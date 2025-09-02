from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, Integer, String, Float, Date, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime


def check_if_table_exists(table):
    conn = BaseHook.get_connection("pg_conn")
    print("Connection URI:", conn.get_uri())

    uri = conn.get_uri().replace("postgres://", "postgresql://")
    engine = create_engine(uri)

    inspector = inspect(engine)
    return str(table).lower() in [t.lower() for t in inspector.get_table_names(schema='public')]

def create_stock_data_table_in_db(stock_data):
    conn = BaseHook.get_connection("pg_conn")
    uri = conn.get_uri().replace("postgres://", "postgresql://")
    engine = create_engine(uri)
    meta = MetaData()

    stock_data_table = Table(
        'MP_stock_prices', meta,
        Column('symbol', String),
        Column('date', Date, primary_key=True),
        Column('open', Float),
        Column('high', Float),
        Column('low', Float),
        Column('close', Float),
        Column('volume', Integer)
    )

    meta.create_all(engine)

def create_news_table_in_db(news_data):
    news_conn = BaseHook.get_connection("pg_conn")
    uri = news_conn.get_uri().replace("postgres://", "postgresql://")
    engine = create_engine(uri)
    meta = MetaData()

    news_table = Table(
        'news_table', meta,
        Column('source_id', String),
        Column('source_name', String),
        Column('author', String),
        Column('title', String),
        Column('description', String),
        Column('url', String, primary_key=True),
        Column('publishedAt', String),
        Column('content', String)
    )

    meta.create_all(engine)


def parse_stock_data(stock_data):
    if not stock_data or "Meta Data" not in stock_data or "Time Series (Daily)" not in stock_data:
        raise ValueError("Incorrect format or missing data")

    symbol = stock_data["Meta Data"]["2. Symbol"]
    time_series = stock_data["Time Series (Daily)"]

    records = []

    for date_str, values in time_series.items():
        records.append({
            'symbol': symbol,
            'date': datetime.strptime(date_str, "%Y-%m-%d").date(),
            'open': float(values["1. open"]),
            'high': float(values['2. high']),
            'low': float(values['3. low']),
            'close': float(values['4. close']),
            'volume': int(values['5. volume'])
        })
   
    return records

def parse_news_data(news_data):
    if not news_data or "articles" not in news_data:
        raise ValueError("Incorrect format or missing data")
    
    records = []
    source_data = news_data["articles"]

    for article in source_data:
        records.append({
            'source_id': article['source']['id'],
            'source_name': article['source']['name'],
            'author': article.get('author'),
            'title': article.get('title'),
            'description': article.get('description'),
            'url': article.get('url'),
            'publishedAt': article.get('publishedAt'),
            'content': article.get('content')
        })
        return records

def update_stocks_table(records, only_latest=False):
    conn = BaseHook.get_connection("pg_conn")
    uri = conn.get_uri().replace("postgres://", "postgresql://")
    engine = create_engine(uri)

    meta = MetaData()
    stock_data_table = Table('MP_stock_prices', meta, autoload_with=engine)

    data_to_insert = records[-1:] if only_latest else records

    with engine.connect() as conn:
        for record in data_to_insert:
            stmt = insert(stock_data_table).values(**record)
            stmt = stmt.on_conflict_do_nothing(index_elements=['date'])
            conn.execute(stmt)

def update_articles_table(records, only_latest=False):
    conn = BaseHook.get_connection("pg_conn")
    uri = conn.get_uri().replace("postgres://", "postgresql://")
    engine = create_engine(uri)

    meta = MetaData()
    stock_data_table = Table('news_table', meta, autoload_with=engine)

    data_to_insert = records[-1:] if only_latest else records

    with engine.connect() as conn:
        for record in data_to_insert:
            stmt = insert(stock_data_table).values(**record)
            stmt = stmt.on_conflict_do_nothing(index_elements=['url'])
            conn.execute(stmt)

def save_to_postgres(df, table_name, if_exists="replace"):
    conn = BaseHook.get_connection("pg_conn")
    uri = conn.get_uri().replace("postgres://", "postgresql://")
    engine = create_engine(uri)
    
    df.to_sql(table_name, engine, index=False, if_exists=if_exists)
    engine.dispose()