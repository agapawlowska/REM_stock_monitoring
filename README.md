# REM Stock Prices Monitoring
A data pipeline designed to monitor REM stock prices and related market news in near real-time.
The project is built with Apache Airflow (Astro runtime) and runs inside Docker containers for portability and reproducibility.

## This pipeline:
- Fetches daily **REM stock price data** from a selected financial API.
- Collects related **news articles** from the News API.
- Analyses daily sentiment.
- Stores raw and processed data in the local Postgres instance.
- Analyses changes in stock prices vs sentiment.
- Plots results and saves them in S3 bucket.

## In Progress:
- Build **dimension and fact tables** for processed stock data
- Implement **business-level metrics and analytics**
- Add **unit and integration tests** for scripts and DAGs

## Project Structure
```bash
REM_stock_monitoring/
├── dags/ # Airflow DAGs for scheduling tasks
├── include/ # Python scripts for data collection and processing
├── *tests/ # Test scripts (in progress)
├── .gitignore
├── .dockerignore
├── Dockerfile
├── docker-compose.override.yml
├── packages.txt
├── requirements.txt
├── README.md
└── docker-compose.yml
```

## Tech Stack
- **Apache Airflow (Astro Runtime)** – task orchestration
- **Docker** – containerised environment
- **Python** – data parsing, transformations, and planned analytics
- **SQL** – storing and managing pipeline outputs
- **NLTK** – sentiment analysis
- **Matplotlib** – data visualisation
- **boto3** – S3 export
