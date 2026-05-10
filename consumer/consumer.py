import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import joblib

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

S3_BUCKET = os.environ["S3_BUCKET"]
S3_MODEL_KEY = os.environ.get("S3_MODEL_KEY", "models/model.pkl")
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
POLL_WAIT_SECONDS = int(os.environ.get("POLL_WAIT_SECONDS", "20"))


def load_model():
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=S3_BUCKET, Key=S3_MODEL_KEY)
    return joblib.load(io.BytesIO(response["Body"].read()))


def write_prediction(s3, record_id: str, prediction: int):
    result = {
        "record_id": record_id,
        "prediction": int(prediction),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    key = f"predictions/{record_id}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(result),
        ContentType="application/json",
    )
    log.info("Wrote prediction to s3://%s/%s", S3_BUCKET, key)


def process_message(sqs, s3, model, message):
    body = json.loads(message["Body"])
    record_id = body["record_id"]
    features = [body["features"]]

    prediction = model.predict(features)[0]
    write_prediction(s3, record_id, prediction)

    sqs.delete_message(
        QueueUrl=SQS_QUEUE_URL,
        ReceiptHandle=message["ReceiptHandle"],
    )
    log.info("Processed and deleted message for %s", record_id)


def main():
    log.info("Loading model from S3...")
    model = load_model()
    log.info("Model loaded. Starting poll loop.")

    sqs = boto3.client("sqs")
    s3 = boto3.client("s3")

    while True:
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=POLL_WAIT_SECONDS,
        )
        messages = response.get("Messages", [])
        if not messages:
            log.info("No messages received, polling again...")
            continue

        for message in messages:
            try:
                process_message(sqs, s3, model, message)
            except Exception:
                log.exception("Failed to process message %s", message.get("MessageId"))


if __name__ == "__main__":
    main()
