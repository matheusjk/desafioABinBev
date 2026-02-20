# json_to_minio.py - Salvar em /opt/airflow/spark_jobs/json_to_minio.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F
import os, requests, time
from unicodedata import normalize
from datetime import datetime
import pandas as pd


def main():
    # Argumentos passados pelo Airflow
    # input_path = (
    #     sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/data/input/dados.json"
    # )
    bucket_name = sys.argv[2] if len(sys.argv) > 2 else "dados-processados"
    minio_endpoint = sys.argv[3] if len(sys.argv) > 3 else "http://minio:9000"
    access_key = sys.argv[4] if len(sys.argv) > 4 else "minioadmin"
    secret_key = sys.argv[5] if len(sys.argv) > 5 else "minioadmin123"

    # Inicializar Spark Session com configurações S3/MinIO
    spark = (
        SparkSession.builder.appName("JSON to MinIO")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    try:
        print("Lendo da API")

        resp = requests.get(
            "https://api.openbrewerydb.org/v1/breweries?page=46&per_page=200"
        )
        cont = 4
        total_elementos = 200
        df_temp = pd.DataFrame()

        while cont > 0:
            resp = requests.get(
                f"https://api.openbrewerydb.org/v1/breweries?page={cont}&per_page={total_elementos}"
            )
            if resp.status_code != 200 or len(resp.json()) < 1 or resp.json() == "[]":
                df_temp = pd.concat(
                    [
                        df_temp,
                        pd.json_normalize(
                            {
                                "id": resp.url,
                                "name": None,
                                "brewery_type": None,
                                "address_1": None,
                                "address_2": None,
                                "address_3": None,
                                "city": None,
                                "state_province": None,
                                "postal_code": None,
                                "country": None,
                                "longitude": None,
                                "latitude": None,
                                "phone": None,
                                "website_url": None,
                                "state": None,
                                "street": None,
                            }
                        ),
                    ],
                    ignore_index=True,
                )
            else:
                df_temp = pd.concat(
                    [df_temp, pd.json_normalize(resp.json())], ignore_index=True
                )
                time.sleep(1)
            cont = cont - 1

        df = spark.createDataFrame(df_temp)

        # gravando dados na camada bronze - totalmente dados brutos
        df.write.format("json").mode("overwrite").save("s3a://breweries/bronze")

        # montando a camada prata - lendo da bronze e convertendo para delta e gravando partitionBy "state_province"
        df_prata = spark.read.format("json").load("s3a://breweries/bronze")

        df_prata.write.format("delta").partitionBy("state_province").save(
            "s3a://breweries/prata"
        )

        # realizando o agrupamento por state_province e brewery_type e contando
        df_ouro = (
            df_prata.groupby("state_province", "brewery_type")
            .count()
            .orderBy(F.desc("count"))
        )  # .show()
        df_ouro.write.format("delta").save("s3a://breweries/ouro")
        print(df_ouro.show())

    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
