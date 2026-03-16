"""
DAG de exemplo integrando Airflow com PostgreSQL, Spark e MinIO
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "data_pipeline_example",
    default_args=default_args,
    description="DAG de exemplo com Spark, PostgreSQL e MinIO",
    schedule_interval="@daily",
    catchup=False,
    tags=["exemplo", "spark", "postgresql"],
)


def tarefa_python():
    """Tarefa Python simples"""
    logger.info("Iniciando tarefa Python")
    logger.info("Stack de Big Data rodando!")
    return "Tarefa Python executada com sucesso"


def tarefa_postgres():
    """Tarefa que interage com PostgreSQL"""
    import psycopg2

    try:
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(
            host="airflow-db",
            database="airflow",
            user="airflow",
            password="airflow123",
        )
        cursor = conn.cursor()

        # Criar tabela de exemplo
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dados_exemplo (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                valor VARCHAR(255)
            )
        """)

        # Inserir dados
        cursor.execute(
            "INSERT INTO dados_exemplo (valor) VALUES (%s)",
            ("Dado inserido pelo Airflow",),
        )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Dados inseridos no PostgreSQL com sucesso")
        return "PostgreSQL: OK"

    except Exception as e:
        logger.error(f"Erro ao conectar com PostgreSQL: {str(e)}")
        raise


def tarefa_minio():
    """Tarefa que interage com MinIO"""
    from minio import Minio
    from io import BytesIO

    try:
        # Conectar ao MinIO
        client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin123",
            secure=False,
        )

        # Criar bucket se não existir
        found = client.bucket_exists("airflow-bucket")
        if not found:
            client.make_bucket("airflow-bucket")
            logger.info("Bucket criado: airflow-bucket")

        # Upload de arquivo
        data = BytesIO(b"Arquivo de teste do Airflow")
        client.put_object(
            "airflow-bucket",
            "teste.txt",
            data,
            length=len(b"Arquivo de teste do Airflow"),
        )

        logger.info("Arquivo enviado para MinIO com sucesso")
        return "MinIO: OK"

    except Exception as e:
        logger.error(f"Erro ao conectar com MinIO: {str(e)}")
        raise


# Tarefas
# task_1 = PythonOperator(
#     task_id="tarefa_inicial",
#     python_callable=tarefa_python,
#     dag=dag,
# )

# task_2 = PythonOperator(
#     task_id="tarefa_postgres",
#     python_callable=tarefa_postgres,
#     dag=dag,
# )

# task_3 = PythonOperator(
#     task_id="tarefa_minio",
#     python_callable=tarefa_minio,
#     dag=dag,
# )

# Tarefa Bash para testar conectividade com Spark
task_4 = BashOperator(
    task_id="check_spark",
    bash_command='curl -s http://spark-master:8080 | grep -q "Spark Master" && echo "Spark Master está online" || echo "Spark Master offline"',
    dag=dag,
)

# Definir dependências
# task_1 >> [task_2, task_3] >> task_4

task_4
