REM Stock Prices Monitoring
A data pipeline designed to monitor REM stock prices and related market news in near real-time.
The project is built with Apache Airflow (Astro runtime) and runs inside Docker containers for portability and reproducibility.

This pipeline:
- Fetches daily REM stock price data from a selected financial API.
- Collects related news articles from the News API.
- Stores raw and processed data in the local Postgres instance.

Currently, the pipeline is focused on data ingestion and storage, with data analysis and export features under active development.


The following features are planned but not yet finalized:
- Sentiment Analysis - 
Apply NLP-based sentiment scoring to collected news articles.
Store sentiment results alongside price data for trend correlation.
- Plotting - 
Generate daily and weekly visualizations of stock prices and sentiment trends.
Use  matplotlib interactive dashboards.
- Export to Amazon S3 - 
Save processed datasets and plots to S3 buckets for long-term storage and external use.

Tech Stack
- Apache Airflow (Astro Runtime) – task orchestration
- Docker – containerized environment
- Python – data parsing, transformations, and planned analytics
- SQL – storing and managing pipeline outputs
- (Planned) NLTK / spaCy / transformers – sentiment analysis
- (Planned) Matplotlib / Plotly – data visualization
- (Planned) boto3 – S3 export
