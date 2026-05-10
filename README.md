# Asynchronous AI Inference System

Airflow + SQS + S3 + Kubernetes pipeline that trains a breast-cancer classifier and serves predictions asynchronously.

**Note:** This README was initially written with AI and then tweaked manually for understanding and verbosity. 

## Prerequisites

- Python 3.10+
- Docker
- A Kubernetes cluster (minikube, kind, or EKS)
- AWS account with S3 bucket and SQS queue created
- AWS CLI installed and configured (`~/.aws/credentials`)
- `kubectl` configured for your cluster

## Project Structure

```
.
├── dags/
│   ├── training_dag.py      # Trains model, saves to S3
│   └── inference_dag.py     # Reads test set, enqueues to SQS
├── consumer/
│   ├── consumer.py          # SQS polling + inference worker
│   ├── requirements.txt
│   └── Dockerfile
└── k8s/
    ├── deployment.yaml      # Kubernetes Deployment
    └── secrets.yaml         # Secret manifest template
```

## Setup

### 1. AWS Resources

Create an S3 bucket and SQS queue (standard queue with long-polling enabled):

```bash
aws s3 mb s3://jacob-astar-seis765-final-project --profile seis765
aws sqs create-queue --queue-name inference-queue --profile seis765
```

Get your SQS queue URL (you'll need it later):

```bash
aws sqs get-queue-url --queue-name inference-queue --profile seis765
```

### 2. Airflow Setup

This project uses the same standalone Airflow setup from HW3/Lab4 — a project-local `AIRFLOW_HOME` backed by SQLite with `SequentialExecutor`. Minor differences exist due to the updated version of Airflow being used. Note that Airflow is running locally on my system, not via Docker or Kubernetes. In a real deployment, this could be ran on an EC2 instance, or via kubernetes with a real persistant database or mounted volume for the SQLite DB. 

#### 2a. Install Airflow into a conda virtualenv

```bash
conda create -n seis765 python==3.10 pip

conda activate seis765

pip install -r requirements.txt
```

#### 2b. Initialize the database and create an admin user

Run these once from the root of this project:

```bash
conda activate seis765

export AIRFLOW_HOME="$(pwd)/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__EXECUTOR="SequentialExecutor"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="admin:ADMIN"
export AWS_PROFILE=seis765

mkdir -p airflow_home
airflow db migrate
```

#### 2c. Set Airflow Variables

Set the S3 bucket and SQS queue URL as Airflow Variables. This can be done via the CLI using the below command:

```bash
airflow variables set S3_BUCKET jacob-astar-seis765-final-project
airflow variables set SQS_QUEUE_URL https://sqs.REGION.amazonaws.com/ACCOUNT_ID/inference-queue
```

#### 2d. Start Airflow

You need three terminals. In each one, activate the conda env and export the same env vars before starting:

```bash
# convenience — run this in each terminal before the commands below
conda activate seis765
export AIRFLOW_HOME="$(pwd)/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__EXECUTOR="SequentialExecutor"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="admin:ADMIN"
export AWS_PROFILE=seis765
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

**Terminal 1 — Scheduler:**

```bash
airflow scheduler
```

**Terminal 2 — DAG Processor:**

```bash
airflow dag-processor
```

**Terminal 3 — API server (UI):**
Note that this command differs from the homework given the updated Airflow version being used. 
```bash
airflow api-server --port 8080 --host 0.0.0.0
```

Open http://localhost:8080 and log in with `admin` and the generated password from the api-server logs (or `cat airflow_home/simple_auth_manager_passwords.json.generated`).

> **Note:** DAGs are discovered directly from the `dags/` folder in this repo — no copying needed. If a DAG shows as paused on first load, toggle it on in the UI.

### 3. Run the Training DAG
DAGs can be triggered via the UI or the CLI using the below commands:


```bash
airflow dags trigger training_dag
```

This will write `models/model.pkl` and `data/test_features.pkl` to S3.

### 4. Run the Inference DAG

Trigger `inference_dag` to populate SQS:

```bash
airflow dags trigger inference_dag
```

### 5. Build and Push the Consumer Image

```bash
cd consumer
docker build -t jastar556/inference-consumer:latest .
docker push jastar556/inference-consumer:latest
```

**NOTE**, the above command uses my Docker Hub repo. You need to use your own, and then update the `image:` field in [k8s/deployment.yaml](k8s/deployment.yaml).

### 6. Deploy to Kubernetes

Copy `k8s/secrets.yaml.template` to `k8s/secrets.yaml` and fill in `k8s/secrets.yaml` with the required base64-encoded values, then apply:

```bash
kubectl apply -f k8s/secrets.yaml
```
This loads the secrets into the kubernetes environment.

You can now deploy the consumer pods, which when running, will start to poll the SQS and process any messages that exist. 

```bash
kubectl apply -f k8s/deployment.yaml
```

Verify pods are running:

```bash
kubectl get pods -l app=inference-consumer
kubectl logs -l app=inference-consumer -f
```

### 7. Scale

```bash
kubectl scale deployment inference-consumer --replicas=4
```

### 8. Verify Results

```bash
aws s3 ls s3://jacob-astar-seis765-final-project/predictions/
aws s3 cp s3://jacob-astar-seis765-final-project/predictions/sample_0000.json -
```

## Teardown

### Kubernetes

Remove the deployment and secrets:

```bash
kubectl delete -f k8s/deployment.yaml
kubectl delete -f k8s/secrets.yaml
```

Verify everything is gone:

```bash
kubectl get pods -l app=inference-consumer
```

### Airflow (local processes)

Stop the three terminal processes (`airflow scheduler`, `airflow dag-processor`, `airflow api-server`) with `Ctrl+C`, then wipe the local state if desired:

```bash
rm -rf airflow_home
```

### AWS Resources

Delete the SQS queue:

```bash
aws sqs delete-queue \
  --queue-url $(aws sqs get-queue-url --queue-name inference-queue --profile seis765 --query QueueUrl --output text) \
  --profile seis765
```

Empty and delete the S3 bucket (emptying is required before deletion):

```bash
aws s3 rm s3://jacob-astar-seis765-final-project --recursive --profile seis765
aws s3 rb s3://jacob-astar-seis765-final-project --profile seis765
```

## Output Format

Each prediction is written to `predictions/<record_id>.json`:

```json
{
  "record_id": "sample_0000",
  "prediction": 1,
  "timestamp": "2026-04-15T12:00:00+00:00"
}
```
