#!/usr/bin/env python3
"""
Job Spark simples para testar integração com Airflow
"""

from pyspark.sql import SparkSession
import sys


def main():
    try:
        spark = SparkSession.builder.appName("AirflowTestJob").getOrCreate()

        print("=" * 50)
        print("🚀 Job Spark iniciado via Airflow")
        print("=" * 50)

        # Cria dados de teste
        data = [
            ("Airflow", "Orquestrador", 2024),
            ("Spark", "Processamento", 2024),
            ("Docker", "Container", 2024),
            ("Teste", "Sucesso", 2024),
        ]

        df = spark.createDataFrame(data, ["Ferramenta", "Tipo", "Ano"])

        print("\n📊 Dados processados:")
        df.show(truncate=False)

        # Operação simples
        count = df.count()
        print(f"\n✅ Total de registros: {count}")

        # Salva resultado (opcional)
        output_path = "/tmp/spark_test_output"
        df.write.mode("overwrite").parquet(output_path)
        print(f"💾 Resultado salvo em: {output_path}")

        spark.stop()
        print("\n🎉 Job concluído com sucesso!")
        return 0

    except Exception as e:
        print(f"\n❌ Erro no job: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
