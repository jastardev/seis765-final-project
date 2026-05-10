from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from io import BytesIO
import numpy as np
import boto3
import joblib


S3_MODEL_KEY = "models/model.pkl"
S3_TEST_KEY = "data/test_features.pkl"

default_args = {"owner": "airflow", "retries": 1}


def load_and_split(**context):

    data = load_breast_cancer()
    X_train, X_test, y_train, y_test = train_test_split(
        data.data, data.target, test_size=0.2, random_state=42
    )
    context["ti"].xcom_push(key="X_train", value=X_train.tolist())
    context["ti"].xcom_push(key="X_test", value=X_test.tolist())
    context["ti"].xcom_push(key="y_train", value=y_train.tolist())
    context["ti"].xcom_push(key="y_test", value=y_test.tolist())


def train_and_save(**context):

    ti = context["ti"]
    X_train = np.array(ti.xcom_pull(task_ids="load_and_split", key="X_train"))
    X_test = ti.xcom_pull(task_ids="load_and_split", key="X_test")
    y_train = np.array(ti.xcom_pull(task_ids="load_and_split", key="y_train"))
    y_test = ti.xcom_pull(task_ids="load_and_split", key="y_test")

    model = LogisticRegression(max_iter=10000)
    model.fit(X_train, y_train)

    s3_bucket = Variable.get("S3_BUCKET")
    s3 = boto3.client("s3")

    model_buf = BytesIO()
    joblib.dump(model, model_buf)
    model_buf.seek(0)
    s3.put_object(Bucket=s3_bucket, Key=S3_MODEL_KEY, Body=model_buf.getvalue())

    test_buf = BytesIO()
    joblib.dump({"X_test": X_test, "y_test": y_test}, test_buf)
    test_buf.seek(0)
    s3.put_object(Bucket=s3_bucket, Key=S3_TEST_KEY, Body=test_buf.getvalue())


with DAG(
    dag_id="training_dag",
    default_args=default_args,
    description="Train breast-cancer classifier and save model + test set to S3",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["training"],
) as dag:

    split_task = PythonOperator(
        task_id="load_and_split",
        python_callable=load_and_split,
    )

    train_task = PythonOperator(
        task_id="train_and_save",
        python_callable=train_and_save,
    )

    split_task >> train_task
