from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import io
import json
import boto3
import joblib

S3_TEST_KEY = "data/test_features.pkl"

default_args = {"owner": "airflow", "retries": 1}


def enqueue_records():
    s3_bucket = Variable.get("S3_BUCKET")
    sqs_queue_url = Variable.get("SQS_QUEUE_URL")

    s3 = boto3.client("s3")
    sqs = boto3.client("sqs")

    response = s3.get_object(Bucket=s3_bucket, Key=S3_TEST_KEY)
    test_data = joblib.load(io.BytesIO(response["Body"].read()))
    X_test = test_data["X_test"]

    for i, features in enumerate(X_test):
        message = {
            "record_id": f"sample_{i:04d}",
            "features": features,
        }
        sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps(message),
        )

    return len(X_test)


with DAG(
    dag_id="inference_dag",
    default_args=default_args,
    description="Enqueue test records from S3 to SQS for async inference",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["inference"],
) as dag:

    enqueue_task = PythonOperator(
        task_id="enqueue_records",
        python_callable=enqueue_records,
    )
