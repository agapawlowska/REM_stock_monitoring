import boto3
import io
from airflow.hooks.base import BaseHook

def export_data_to_s3(df):
    s3_conn_id="S3_MP"
    conn = BaseHook.get_connection(s3_conn_id)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
    )

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket="mp-stock-prices-monitoring",
        Key="merged_stock_sentiment.csv",
        Body=csv_buffer.getvalue()
    )

