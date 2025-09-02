FROM astrocrpublic.azurecr.io/runtime:3.0-4

COPY .env /usr/local/airflow/.env

RUN python -m nltk.downloader punkt punkt_tab stopwords wordnet vader_lexicon -d /usr/local/airflow/nltk_data
ENV NLTK_DATA=/usr/local/airflow/nltk_data


