import matplotlib.pyplot as plt
from airflow.hooks.base import BaseHook
import boto3
import io

def plot_and_save_changes(df, s3_bucket, s3_key, s3_conn_id):
    df["open_pct_change"] = df["open"].pct_change() * 100
    df["close_pct_change"] = df["close"].pct_change() * 100
    df["high_pct_change"] = df["high"].pct_change() * 100
    df = df.dropna(subset=["sentiment", "open_pct_change", "close_pct_change", "high_pct_change"])

    fig, ax1 = plt.subplots(figsize=(12,6))

    ax1.plot(df["date"], df["open_pct_change"], label="Open % change", marker="o")
    ax1.plot(df["date"], df["close_pct_change"], label="Close % change", marker="o")
    ax1.plot(df["date"], df["high_pct_change"], label="High % change", marker="o")

    ax1.set_xlabel("Data")
    ax1.set_ylabel("Change [%]")
    ax1.legend(loc="upper left")
    ax1.grid(True)

    ax2 = ax1.twinx()
    ax2.plot(df["date"], df["sentiment"], color="black", linestyle="--", marker="x", label="Sentiment (compound)")

    colors = df["sentiment_label"].map({"positive":"green", "negative":"red", "neutral":"gray"})
    ax2.scatter(df["date"], df["sentiment"], c=colors, s=80, label="Sentiment label")

    ax2.set_ylabel("Sentiment (compound)")

    fig.suptitle("Stock prices change vs sentiment", fontsize=14)
    fig.tight_layout()

    conn = BaseHook.get_connection(s3_conn_id)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
    )

    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=buf)
    plt.close(fig)
        

    return f"Plot uploaded to s3://{s3_bucket}/{s3_key}"