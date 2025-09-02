from airflow.decorators import dag, task, task_group
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration
import pandas as pd
from include.data_importer import fetch_stock_API, fetch_news_API
from include.db_utils import save_to_postgres, check_if_table_exists, create_stock_data_table_in_db, parse_stock_data, update_stocks_table, parse_news_data, create_news_table_in_db, update_articles_table
from include.sentiment_analysis import get_data_from_postgres, get_sentiment
from include.data_exporter import export_data_to_s3
from include.plot_generator import plot_and_save_changes


@dag(
    start_date=datetime(2025, 7, 19), 
    schedule="0 18 * * 1-5",
    tags = ['REM_stock_sentiment'],
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': duration(minutes=5)
    }
)

def rem_stock_sentiment():

    
    @task_group(group_id='fetch_data')
    def collect_data_group():
        @task
        def fetch_stock_data():
            return fetch_stock_API()

        @task 
        def fetch_news_data():
            return fetch_news_API()

        stock_data = fetch_stock_data()
        news_data = fetch_news_data()

        return stock_data, news_data
    
    @task_group(group_id="stock_data_table")
    def stock_data_table(stock_data):
        
        def decide_table_path(stock_data):
            table_name = "MP_stock_prices"  
            table_exists = check_if_table_exists(table_name)
            if table_exists:
                return "stock_data_table.update_stock_table"
            else:
                return "stock_data_table.create_stock_table"


        # 2. Branch operator 
        branch = BranchPythonOperator(
            task_id="check_if_table_exists",
            python_callable=decide_table_path,
            op_args=[stock_data]
        )
        @task 
        def update_stock_table(stock_data):
            records = parse_stock_data(stock_data)
            update_stocks_table(records, only_latest=False)

        @task
        def create_stock_data_table(stock_data):
            create_stock_data_table_in_db(stock_data)
            records = parse_stock_data(stock_data)
            create_stock_data_table(records, only_latest=False)

        join = EmptyOperator(task_id="join_stock_table",
                             trigger_rule="none_failed_min_one_success")


        stock_updated = update_stock_table(stock_data)
        stock_created = create_stock_data_table(stock_data)


        branch >> [stock_updated, stock_created] >> join

        return join 

    @task_group(group_id="news_table")
    def news_table(news_data):
            def decide_table_path(news_data):
                table_name = "news_table" 
                table_exists = check_if_table_exists(table_name)
                if table_exists:
                    return "news_table.update_news_table"
                else:
                    return "news_table.create_news_table"


            branch = BranchPythonOperator(
                task_id="check_if_table_exists",
                python_callable=decide_table_path,
                op_args=[news_data]
            )

            @task 
            def update_news_table(news_data):
                print("Update the table...")
                records = parse_news_data(news_data)
                update_articles_table(records,  only_latest=False)


            @task
            def create_news_table(news_data):
                create_news_table_in_db(news_data)

            join = EmptyOperator(task_id="join_news_table",
                                trigger_rule="none_failed_min_one_success")

            news_created = create_news_table(news_data)
            news_updated = update_news_table(news_data)

            branch >> [news_created, news_updated] >> join

            return join 

    @task
    def analyze_sentiment():
        df_news = get_data_from_postgres("news_table")
        df_stock = get_data_from_postgres("MP_stock_prices")

        df_news_sentiment = get_sentiment(df_news)

        df_stock['date_only'] = pd.to_datetime(df_stock['date']).dt.date

        df_stock_with_sentiment = df_stock.merge(
                                                    df_news_sentiment[['date_only_shifted','sentiment', 'sentiment_label']],
                                                    left_on='date_only',
                                                    right_on='date_only_shifted',
                                                    how='left'
                                                )
        save_to_postgres(df_stock_with_sentiment, "merged_stock_sentiment")

        return f"Merged {len(df_stock_with_sentiment)} rows saved to Postgres"
   
    @task 
    def plot_the_data():
        df_stock_with_sentiment = get_data_from_postgres("merged_stock_sentiment")

        return plot_and_save_changes(
        df_stock_with_sentiment,
        s3_bucket="mp-stock-prices-monitoring",
        s3_key="plots/stock_vs_sentiment.png",
        s3_conn_id="S3_MP"
    )

    @task
    def save_data_to_s3():
        df_stock_with_sentiment = get_data_from_postgres("merged_stock_sentiment")
        export_data_to_s3(df_stock_with_sentiment)
        return f"{len(df_stock_with_sentiment)} rows exported to S3"



    task_1_stock_data, task_1_news_data = collect_data_group()
    task_2_1 = stock_data_table(task_1_stock_data) 
    task_2_2 = news_table(task_1_news_data) 
    task_3 = analyze_sentiment()
    task_4_1 = save_data_to_s3()
    task_4_2 = plot_the_data()


    task_1_stock_data >> task_2_1 >> task_3 >> task_4_1
    task_1_news_data >> task_2_2 >> task_3 >> task_4_2 

    

dag = rem_stock_sentiment()


