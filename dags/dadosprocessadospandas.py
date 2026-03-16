from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator

# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO

import os, requests, time
from unicodedata import normalize
import pandas as pd

from sqlalchemy import create_engine

# import boto3
# from botocore.exceptions import ClientError

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(year=2025, day=31, month=12, hour=0, minute=0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=5),
}


# Configurações do MinIO (S3 compatível)
MINIO_ENDPOINT = "http://minio:9000"  # Ajuste conforme seu docker network
MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin123"
AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin123"
AWS_S3_ENDPOINT_URL = "http://minio:9000"
MINIO_BUCKET = "dados-processados"


def write_to_minio():
    """
    Escreve dados em um bucket MinIO usando S3Hook
    """
    # Dados de exemplo (pode vir de XCom ou processamento anterior)
    try:
        print("Lendo da API")

        resp = requests.get(
            "https://api.openbrewerydb.org/v1/breweries?page=46&per_page=200"
        )
        # cont = 4
        # total_elementos = 200
        # df_temp = pd.DataFrame()

        # while cont > 0:
        #     resp = requests.get(
        #         f"https://api.openbrewerydb.org/v1/breweries?page={cont}&per_page={total_elementos}"
        #     )
        #     if resp.status_code != 200 or len(resp.json()) < 1 or resp.json() == "[]":
        #         df_temp = pd.concat(
        #             [
        #                 df_temp,
        #                 pd.json_normalize(
        #                     {
        #                         "id": resp.url,
        #                         "name": None,
        #                         "brewery_type": None,
        #                         "address_1": None,
        #                         "address_2": None,
        #                         "address_3": None,
        #                         "city": None,
        #                         "state_province": None,
        #                         "postal_code": None,
        #                         "country": None,
        #                         "longitude": None,
        #                         "latitude": None,
        #                         "phone": None,
        #                         "website_url": None,
        #                         "state": None,
        #                         "street": None,
        #                     }
        #                 ),
        #             ],
        #             ignore_index=True,
        #         )
        #     else:
        #         df_temp = pd.concat(
        #             [df_temp, pd.json_normalize(resp.json())], ignore_index=True
        #         )
        #         time.sleep(1)
        #     cont = cont - 1

        # df = spark.createDataFrame(df_temp)

        # gravando dados na camada bronze - totalmente dados brutos
        # df.write.format("json").mode("overwrite").save("s3a://breweries/bronze")

        # montando a camada prata - lendo da bronze e convertendo para delta e gravando partitionBy "state_province"
        # df_prata = spark.read.format("json").load("s3a://breweries/bronze")

        # df_prata.write.format("delta").partitionBy("state_province").save(
        #     "s3a://breweries/prata"
        # )

        # # realizando o agrupamento por state_province e brewery_type e contando
        # df_ouro = (
        #     df_prata.groupby("state_province", "brewery_type")
        #     .count()
        #     .orderBy(F.desc("count"))
        # )  # .show()
        # df_ouro.write.format("delta").save("s3a://breweries/ouro")
        # print(df_ouro.show())

        # df = pd.DataFrame(data)
        print("ENTRANDO NA PARTE DE CONEXÃO POSTGRES")

        # Create connection string (replace with your credentials)
        engine = create_engine(
            "postgresql+psycopg2://airflow:airflow123@airflow-db/airflow"
        )
        df_temp = pd.json_normalize(resp.json())

        # Write DataFrame to PostgreSQL table
        df_temp.to_sql("bronze_breweries", con=engine, if_exists="replace", index=False)
        print("FIM DA PARTE DE GRAVAÇÃO POSTGRES")
        # Criar cliente S3
        # s3_client = boto3.client(
        #     "s3a",
        #     endpoint_url=MINIO_ENDPOINT,
        #     aws_access_key_id=AWS_ACCESS_KEY_ID,
        #     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        # )

        # # Criar bucket se não existir
        # try:
        #     s3_client.head_bucket(Bucket=MINIO_BUCKET)
        # except ClientError:
        #     s3_client.create_bucket(Bucket=MINIO_BUCKET)
        #     print(f"Bucket '{MINIO_BUCKET}' criado!")

        # # Upload de arquivo local
        # # s3_client.upload_file('/path/local/arquivo.txt', bucket_name, 'remoto/arquivo.txt')

        # # Upload de string/conteúdo em memória
        # s3_client.put_object(
        #     Bucket=MINIO_BUCKET,
        #     Key="dados/output.json",
        #     Body=df_temp.to_json().encode("utf-8"),
        #     ContentType="text/plain",
        # )

        # print(f"Dados enviados para {MINIO_BUCKET}/dados/output.txt")

        # # Retornar informações para XCom
        # # return {"bucket": bucket_name, "key": key, "rows": len(df)}

    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")
        raise


# def write_string_to_minio(**kwargs):
#     """
#     Escreve uma string diretamente para o MinIO
#     """
#     s3_hook = S3Hook(aws_conn_id="minio_conn")

#     conteudo = "Este é um conteúdo de exemplo gerado pelo Airflow!"
#     bucket_name = "meu-bucket"
#     key = "arquivos/exemplo.txt"

#     # Usar load_string para conteúdo texto
#     s3_hook.load_string(
#         string_data=conteudo, key=key, bucket_name=bucket_name, replace=True
#     )

#     print(f"String salva em s3://{bucket_name}/{key}")


with DAG(
    "minio_write_dag",
    default_args=default_args,
    description="DAG de exemplo para escrita no MinIO",
    schedule_interval=None,
    catchup=False,
    tags=["minio", "s3", "etl"],
) as dag:
    t1 = DummyOperator(task_id="extract_data", dag=dag)

    task_run_simple_script = BashOperator(
        task_id="run_simple_script",
        bash_command="python /home/matheus/Documentos/ABinBev/dags/teste_write_postgres.py",
    )

    # # Task 1: Escrever DataFrame como CSV
    # task_write_csv = PythonOperator(
    #     task_id="write_csv_to_minio",
    #     python_callable=write_to_minio,
    # )

    # Definir ordem das tasks
    t1 >> task_run_simple_script
