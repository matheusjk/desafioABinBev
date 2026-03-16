from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import json, os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Configurações do MinIO (S3 compatível)
MINIO_ENDPOINT = "http://minio:9000"  # Ajuste conforme seu docker network
MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin123"
AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin123"
AWS_S3_ENDPOINT_URL = "http://minio:9000"
MINIO_BUCKET = "dados-processados"

JOBS_PATH = "/opt/spark/work-dir/processa.py"
PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.3.2,org.postgresql:postgresql:42.7.4"


def spark_submit(job_file):
    return "docker exec spark-master /opt/spark/bin/spark-submit --master local[*] --deploy-mode client  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000   --conf spark.hadoop.fs.s3a.access.key=minioadmin  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123   --conf spark.hadoop.fs.s3a.path.style.access=true  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false --conf spark.dynamicAllocation.enabled=false --conf spark.shuffle.service.enabled=false --driver-memory 1G --executor-memory 1G --num-executors 1 --executor-cores 1  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.3.2,org.postgresql:postgresql:42.7.4  processa.py teste"


def spark_upload(file):
    return f"cp airflow-webserver:/opt/airflow/dags/{file} /data/"
    # docker cp airflow-webserver:/opt/airflow/dags/{file} spark-master:/data/{file}


with DAG(
    "spark_json_to_minio",
    default_args=default_args,
    description="Lê JSON e grava no MinIO usando Spark",
    schedule_interval=None,  # DAG manual ou ajuste conforme necessidade
    start_date=days_ago(1),
    tags=["spark", "minio", "json", "etl"],
) as dag:
    # envia_script = BashOperator(
    #     task_id="envia_arquivo",
    #     bash_command=spark_upload("/home/matheus/Documentos/ABinBev/dags/processa.py"),
    # )

    bronze_categories = BashOperator(
        task_id="bronze_tbcategories0",
        bash_command=spark_submit("processa.py"),
    )

    # envia_script
    bronze_categories
