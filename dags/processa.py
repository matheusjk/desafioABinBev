from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import FloatType, StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F
import requests, time, os
from unicodedata import normalize
from datetime import datetime
import pandas as pd


def main():

    # bucket_name = sys.argv[2] if len(sys.argv) > 2 else "breweries"
    # minio_endpoint = sys.argv[3] if len(sys.argv) > 3 else "http://minio:9000"
    # access_key = sys.argv[4] if len(sys.argv) > 4 else "minioadmin"
    # secret_key = sys.argv[5] if len(sys.argv) > 5 else "minioadmin"

    spark = (
        SparkSession.builder.appName("SparkToMinIOPipeline")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    # ---- Safety net: JVM-level override ----
    hconf = spark._jsc.hadoopConfiguration()

    hconf.set("fs.s3a.access.key", "minioadmin")  # Your MinIO key
    hconf.set("fs.s3a.secret.key", "minioadmin123")  # Your MinIO secret
    hconf.set("fs.s3a.endpoint", "http://minio:9000")  # OR your host/IP
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    try:
        print("Lendo da API")

        resp = requests.get(
            "https://api.openbrewerydb.org/v1/breweries?page=46&per_page=200"
        )
        cont = 40
        total_elementos = 200
        df_temp = pd.DataFrame()

        while cont > 0:
            resp = requests.get(
                f"https://api.openbrewerydb.org/v1/breweries?page={cont}&per_page={total_elementos}"
            )

            df_temp = pd.concat(
                [df_temp, pd.json_normalize(resp.json())], ignore_index=True
            )
            time.sleep(1)
            cont = cont - 1

        df_temp.dropna(inplace=True)
        print("DADOS DA API JSON: ", df_temp.head(15))

        df = spark.createDataFrame(df_temp)
        # print(df.show(2))

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

        df_prata.write.format("jdbc").option(
            "url", "jdbc:postgresql://postgres-pgvector:5432/datalake_raw"
        ).option("dbtable", "prata").option("user", "datalake").option(
            "password", "datalake_secure_pass"
        ).option("driver", "org.postgresql.Driver").mode("overwrite").save()

        # postgresql+psycopg2://airflow:airflow123@airflow-db/airflow

    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
