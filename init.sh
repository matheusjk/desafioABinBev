init_script = '''#!/bin/bash
# Script de inicialização do Data Platform
# Executar: chmod +x init.sh && ./init.sh

echo "=============================================="
echo "DATA PLATFORM - INICIALIZAÇÃO"
echo "=============================================="

# Criar diretórios necessários
mkdir -p dags logs plugins config spark-jars

# Download do driver JDBC PostgreSQL para Spark
if [ ! -f "spark-jars/postgresql-42.6.0.jar" ]; then
    echo "Baixando driver PostgreSQL JDBC..."
    wget -P spark-jars https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
fi

# Download de dependências S3A para Spark (Hadoop AWS)
if [ ! -f "spark-jars/hadoop-aws-3.3.4.jar" ]; then
    echo "Baixando dependências Hadoop AWS..."
    wget -P spark-jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
    wget -P spark-jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
fi

echo ""
echo "=============================================="
echo "INICIANDO SERVIÇOS DOCKER"
echo "=============================================="

# Subir os serviços
docker-compose up -d

echo ""
echo "=============================================="
echo "AGUARDANDO INICIALIZAÇÃO..."
echo "=============================================="

# Aguardar PostgreSQL
echo "Aguardando PostgreSQL..."
sleep 10

# Aguardar MinIO
echo "Aguardando MinIO..."
sleep 5

# Configurar MinIO (criar buckets)
echo "Configurando buckets no MinIO..."
docker run --rm --network data_platform \
  -e MC_HOST_minio=http://minioadmin:minioadmin123@minio:9000 \
  minio/mc:latest \
  mb minio/spark-bucket minio/airflow-bucket minio/metabase-bucket || true

echo ""
echo "=============================================="
echo "SERVIÇOS DISPONÍVEIS:"
echo "=============================================="
echo "MinIO Console:    http://localhost:9001 (minioadmin/minioadmin123)"
echo "Metabase:         http://localhost:3000"
echo "Airflow UI:       http://localhost:8082 (admin/admin)"
echo "Spark Master UI:  http://localhost:8080"
echo "Spark Worker UI:  http://localhost:8081"
echo "PostgreSQL:       localhost:5432 (admin/admin123)"
echo ""
echo "Para ver logs: docker-compose logs -f [serviço]"
echo "Para parar: docker-compose down"
echo "=============================================="
'''

with open('init.sh', 'w', encoding='utf-8') as f:
    f.write(init_script)

print("✓ Script de inicialização criado: init.sh")
print("✓ Execute: chmod +x init.sh && ./init.sh")