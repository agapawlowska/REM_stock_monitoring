from airflow.decorators import dag, task, task_group
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration
from include.data_importer import fetch_stock_API, fetch_news_API
from include.db_utils import check_if_table_exists, create_stock_data_table_in_db, parse_stock_data, update_table, parse_news_data, create_news_table_in_db



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
        # 1. Funkcja sprawdzająca – co zwrócić
        def decide_table_path(stock_data):
            table_name = "MP_stock_prices"  # lub pobierz z konfiguracji, parametru, albo stałej
            table_exists = check_if_table_exists(table_name)
            if table_exists:
                return "stock_data_table.update_stock_table"
            else:
                return "stock_data_table.create_stock_table"


        # 2. Branch operator (zwykły sposób)
        branch = BranchPythonOperator(
            task_id="check_if_table_exists",
            python_callable=decide_table_path,
            op_args=[stock_data]
        )
        @task 
        def update_stock_table(stock_data):
            records = parse_stock_data(stock_data)
            update_table(records, "MP_stock_prices", only_latest=True)

        @task
        def create_stock_data_table(stock_data):
            create_stock_data_table_in_db(stock_data)
            records = parse_stock_data(stock_data)
            create_stock_data_table(records, only_latest=False)


        # 4. Punkt wspólny po if/else
        join = EmptyOperator(task_id="join_stock_table",
                             trigger_rule="none_failed_min_one_success")


        stock_updated = update_stock_table(stock_data)
        stock_created = create_stock_data_table(stock_data)

        # 5. Kolejność
        branch >> [stock_updated, stock_created] >> join

        return join 

    @task_group(group_id="news_table")
    def news_table(news_data):
            # 1. Funkcja sprawdzająca – co zwrócić
            def decide_table_path(news_data):
                table_name = "news_table" 
                table_exists = check_if_table_exists(table_name)
                if table_exists:
                    return "news_table.update_news_table"
                else:
                    return "news_table.create_news_table"

            # 2. Branch operator (zwykły sposób)
            branch = BranchPythonOperator(
                task_id="check_if_table_exists",
                python_callable=decide_table_path,
                op_args=[news_data]
            )

            @task 
            def update_news_table(news_data):
                print("Update the table...")
                records = parse_news_data(news_data)
                update_table(records, "news_table", only_latest=True)


            @task
            def create_news_table(news_data):
                create_news_table_in_db(news_data)

            join = EmptyOperator(task_id="join_news_table",
                                 trigger_rule="none_failed_min_one_success")

            news_created = create_news_table(news_data)
            news_updated = update_news_table(news_data)

            branch >> [news_created, news_updated] >> join

            return join  # <-- zwracamy ostatni task

    @task
    def analyze_sentiment():
        print("Analizuję sentyment...")

    
    @task 
    def plot_the_data():
        print("Wykres zmiany vs sentymentu...")


    @task
    def save_data_to_s3():
        print("Zapisuję dane w S3 bucket...")

    @task
    def save_plot_to_s3():
        print("Zapisuję wykres w S3 bucket...")

    task_1_stock_data, task_1_news_data = collect_data_group()
    task_2_1 = stock_data_table(task_1_stock_data) 
    task_2_2 = news_table(task_1_news_data) 
    task_3 = analyze_sentiment()
    task_4_1 = save_data_to_s3()
    task_4_2 = plot_the_data()
    task_5 = save_plot_to_s3()

    task_1_stock_data >> task_2_1 >> task_3 >> task_4_1
    task_1_news_data >> task_2_2 >> task_3 >> task_4_2 >> task_5

    

dag = rem_stock_sentiment()


