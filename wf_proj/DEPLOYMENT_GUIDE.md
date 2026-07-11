# DEPLOYMENT GUIDE

## Prerequisites

### System Requirements
- **OS**: Linux (Ubuntu 20.04+), macOS, or Windows with WSL2
- **Python**: 3.9 or higher
- **PostgreSQL**: 12 or higher
- **AWS Account**: With S3 access and KMS keys
- **RAM**: Minimum 8GB (16GB recommended)
- **CPU**: Minimum 4 cores (8 cores recommended)

### Network Requirements
- Connectivity to all source databases
- Connectivity to PostgreSQL metadata database
- Connectivity to AWS S3 and KMS services
- Firewall rules configured for data connector ports

---

## Installation Steps

### 1. Clone Repository

```bash
git clone https://github.com/your-org/data-engineering-framework.git
cd data-engineering-framework
```

### 2. Create Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### 3. Install Python Dependencies

```bash
# Install all required packages
pip install -r requirements.txt

# Install optional packages for specific connectors
pip install pyodbc cx_Oracle pymongo openpyxl zeep
```

### 4. Setup PostgreSQL Metadata Database

```bash
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE metadata_management;

# Exit psql
\q

# Run schema creation script
psql -U postgres -d metadata_management -f sql_scripts/metadata_schema.sql

# Verify tables created
psql -U postgres -d metadata_management -c "\dt"
```

### 5. Configure Application Settings

```bash
# Copy template configuration
cp config/config.template.yaml config/config.yaml

# Edit configuration
nano config/config.yaml
```

**Configuration Example:**

```yaml
# config/config.yaml

database:
  metadata_db:
    host: localhost
    port: 5432
    database: metadata_management
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
    pool_size: 10

aws:
  s3:
    bucket: data-lake
    region: us-east-1
    access_key_id: ${AWS_ACCESS_KEY}
    secret_access_key: ${AWS_SECRET_KEY}
  kms:
    key_id: arn:aws:kms:us-east-1:xxxx:key/xxxxx
    region: us-east-1

framework:
  max_retries: 5
  retry_backoff_multiplier: 2.0
  circuit_breaker_threshold: 5
  circuit_breaker_timeout_seconds: 300
  batch_size: 100000
  default_chunk_size_mb: 256

logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file: logs/framework.log
  max_size_mb: 100
  backup_count: 10

sources:
  sqlserver_prod:
    host: sqlserver-prod.internal
    port: 1433
    username: ${SQLSERVER_USERNAME}
    password: ${SQLSERVER_PASSWORD}
  
  oracle_prod:
    host: oracle-prod.internal
    port: 1521
    username: ${ORACLE_USERNAME}
    password: ${ORACLE_PASSWORD}
```

### 6. Set Environment Variables

```bash
# Create .env file
cat > .env << EOF
# PostgreSQL
DB_USERNAME=data_admin
DB_PASSWORD=secure_password_123

# AWS Credentials
AWS_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# Source System Credentials
SQLSERVER_USERNAME=svc_extract
SQLSERVER_PASSWORD=secure_password_456

ORACLE_USERNAME=svc_extract
ORACLE_PASSWORD=secure_password_789

# Framework Settings
LOG_LEVEL=INFO
EOF

# Load environment variables
export $(cat .env | xargs)
```

### 7. Test Installation

```bash
# Run tests
python -m pytest tests/ -v

# Run specific test
python -m pytest tests/test_connectors.py -v

# Run example
python examples/simple_extraction_example.py
```

---

## AWS Setup

### S3 Bucket Creation

```bash
# Create S3 bucket
aws s3 mb s3://data-lake --region us-east-1

# Create bucket structure
aws s3api put-object --bucket data-lake --key raw/
aws s3api put-object --bucket data-lake --key curated/
aws s3api put-object --bucket data-lake --key archive/

# Enable versioning
aws s3api put-bucket-versioning \
  --bucket data-lake \
  --versioning-configuration Status=Enabled

# Enable encryption
aws s3api put-bucket-encryption \
  --bucket data-lake \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "aws:kms",
        "KMSMasterKeyID": "arn:aws:kms:us-east-1:xxxx:key/xxxxx"
      }
    }]
  }'

# Set lifecycle policy (archive after 2 years)
aws s3api put-bucket-lifecycle-configuration \
  --bucket data-lake \
  --lifecycle-configuration file://s3_lifecycle.json
```

### IAM Role Setup

```bash
# Create IAM role for framework
aws iam create-role --role-name DataEngineeringFramework \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach S3 policy
aws iam put-role-policy --role-name DataEngineeringFramework \
  --policy-name S3Access \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::data-lake/*",
        "arn:aws:s3:::data-lake"
      ]
    }]
  }'

# Attach KMS policy
aws iam put-role-policy --role-name DataEngineeringFramework \
  --policy-name KMSAccess \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "kms:GenerateDataKey"
      ],
      "Resource": "arn:aws:kms:us-east-1:xxxx:key/xxxxx"
    }]
  }'
```

---

## Docker Deployment

### Build Docker Image

```dockerfile
# Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create logs directory
RUN mkdir -p logs

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')" || exit 1

# Run application
CMD ["python", "-m", "framework.main"]
```

### Build and Run

```bash
# Build image
docker build -t data-engineering-framework:1.0 .

# Run container
docker run -d \
  --name framework \
  -e DB_HOST=postgres-host \
  -e DB_PORT=5432 \
  -e AWS_ACCESS_KEY_ID=${AWS_KEY} \
  -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET} \
  -v /etc/framework/config.yaml:/app/config/config.yaml \
  -v /var/log/framework:/app/logs \
  data-engineering-framework:1.0

# View logs
docker logs -f framework
```

### Docker Compose Setup

```yaml
# docker-compose.yml
version: '3.8'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: metadata_management
      POSTGRES_USER: data_admin
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql_scripts/metadata_schema.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5432:5432"
    networks:
      - framework

  framework:
    build: .
    depends_on:
      - postgres
    environment:
      DB_HOST: postgres
      DB_PORT: 5432
      DB_NAME: metadata_management
      DB_USER: data_admin
      DB_PASSWORD: ${DB_PASSWORD}
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_KEY}
    volumes:
      - ./config:/app/config
      - ./logs:/app/logs
    networks:
      - framework

volumes:
  postgres_data:

networks:
  framework:
    driver: bridge
```

**Start services:**

```bash
docker-compose up -d
```

---

## Kubernetes Deployment

### Create Kubernetes Manifests

```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: data-engineering

---

# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: framework-config
  namespace: data-engineering
data:
  config.yaml: |
    database:
      metadata_db:
        host: postgres.data-engineering.svc.cluster.local
        port: 5432
        database: metadata_management

---

# k8s/secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: framework-secrets
  namespace: data-engineering
type: Opaque
data:
  db-password: <base64-encoded-password>
  aws-key: <base64-encoded-aws-key>
  aws-secret: <base64-encoded-aws-secret>

---

# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: framework
  namespace: data-engineering
spec:
  replicas: 3
  selector:
    matchLabels:
      app: framework
  template:
    metadata:
      labels:
        app: framework
    spec:
      containers:
      - name: framework
        image: data-engineering-framework:1.0
        imagePullPolicy: Always
        env:
        - name: DB_HOST
          value: postgres.data-engineering.svc.cluster.local
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: framework-secrets
              key: db-password
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: framework-secrets
              key: aws-key
        resources:
          requests:
            memory: "2Gi"
            cpu: "500m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
```

**Deploy to Kubernetes:**

```bash
# Create namespace
kubectl apply -f k8s/namespace.yaml

# Create secrets and config
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml

# Deploy application
kubectl apply -f k8s/deployment.yaml

# Check deployment status
kubectl get deployment -n data-engineering
kubectl get pods -n data-engineering
kubectl logs -n data-engineering deployment/framework
```

---

## Scheduling with Airflow

### Create Airflow DAG

```python
# dags/data_extraction_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    'data_extraction_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False
)

def extract_sqlserver(**context):
    from framework.models.base_models import ExecutionRequest
    from framework.controllers.pipeline_controller import PipelineController
    
    request = ExecutionRequest(
        pipeline_id=1,
        execution_type='SCHEDULED',
        triggered_by='AIRFLOW'
    )
    
    controller = PipelineController(...)
    response = controller.execute_pipeline(request)
    return response

task = PythonOperator(
    task_id='extract_sqlserver',
    python_callable=extract_sqlserver,
    dag=dag
)
```

---

## Monitoring & Alerts

### CloudWatch Monitoring

```python
import boto3

def setup_cloudwatch_alarms():
    cloudwatch = boto3.client('cloudwatch')
    
    # Pipeline failure rate
    cloudwatch.put_metric_alarm(
        AlarmName='DataEngineeringFramework-HighFailureRate',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=1,
        MetricName='FailedExecutions',
        Namespace='DataEngineeringFramework',
        Period=300,
        Statistic='Sum',
        Threshold=5,
        ActionsEnabled=True,
        AlarmActions=['arn:aws:sns:us-east-1:xxxx:alerts']
    )
    
    # Execution time anomaly
    cloudwatch.put_metric_alarm(
        AlarmName='DataEngineeringFramework-SlowExecution',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=2,
        MetricName='ExecutionDurationSeconds',
        Namespace='DataEngineeringFramework',
        Period=300,
        Statistic='Average',
        Threshold=1200,  # 20 minutes
        ActionsEnabled=True,
        AlarmActions=['arn:aws:sns:us-east-1:xxxx:alerts']
    )

setup_cloudwatch_alarms()
```

### Prometheus Metrics

```python
from prometheus_client import Counter, Histogram, start_http_server

# Metrics
extraction_count = Counter(
    'framework_extractions_total',
    'Total extraction attempts',
    ['pipeline', 'status']
)

extraction_duration = Histogram(
    'framework_extraction_duration_seconds',
    'Extraction duration in seconds',
    ['pipeline']
)

# Start metrics server
start_http_server(8000)
```

---

## Verification Checklist

- [ ] PostgreSQL database created and schema imported
- [ ] AWS S3 bucket created with proper permissions
- [ ] AWS KMS key configured and accessible
- [ ] Configuration file completed with all credentials
- [ ] Python dependencies installed successfully
- [ ] Tests pass: `pytest tests/`
- [ ] Example pipeline runs successfully
- [ ] Logs are being generated properly
- [ ] Monitoring alerts are configured
- [ ] Documentation reviewed and understood

---

## Troubleshooting

### PostgreSQL Connection Error

```bash
# Test connection
psql -h localhost -U data_admin -d metadata_management

# Check credentials in config.yaml
cat config/config.yaml | grep -A 5 "metadata_db"
```

### AWS Credentials Error

```bash
# Verify AWS credentials
aws sts get-caller-identity

# Check IAM permissions
aws iam get-user-policy --user-name data-extraction-user --policy-name S3Access
```

### Framework Import Error

```bash
# Check Python path
echo $PYTHONPATH

# Add to path if needed
export PYTHONPATH=$PYTHONPATH:/path/to/framework
```

---

**Deployment Guide Version**: 1.0  
**Status**: Production Ready  
**Updated**: January 2024
