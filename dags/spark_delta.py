from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "spark_delta_minio",
    default_args=default_args,
    description="Exemplo Spark com Delta Lake e MinIO",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["spark", "delta", "minio"],
) as dag:
    spark_job = SparkSubmitOperator(
        task_id="process_delta",
        application="/opt/airflow/dags/scripts/delta_job.py",
        conn_id="spark_default0",
        verbose=True,
        env_vars={
            "SPARK_HOME": "/opt/spark",  # ← Importante!
            "PATH": "/opt/spark/bin:" + os.environ.get("PATH", ""),
            "PYSPARK_PYTHON": "python3",
        },
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client",
        },
    )

    spark_job
