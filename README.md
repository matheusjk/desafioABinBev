# 🚀 Data Stack: Airflow 2.10 · Spark + Delta · MinIO · Metabase · PostgreSQL

## 📦 Serviços & Portas

| Serviço               | Porta(s)        | Usuário / Senha padrão         |
|-----------------------|-----------------|--------------------------------|
| Airflow Webserver     | **8080**        | admin / admin123               |
| Airflow Flower        | **5555**        | —                              |
| Spark Master UI       | **8081**        | —                              |
| Spark Worker UI       | **8082**        | —                              |
| Spark History Server  | **18080**       | —                              |
| MinIO API             | **9000**        | minioadmin / minioadmin123     |
| MinIO Console         | **9001**        | minioadmin / minioadmin123     |
| Metabase              | **3000**        | (setup inicial via browser)    |
| PostgreSQL (Airflow)  | **5432**        | airflow / airflow_secret_2024  |
| PostgreSQL (Metabase) | **5433**        | metabase / metabase_secret_2024|
| Redis                 | **6379**        | —                              |

---

## 🛠️ Setup rápido

### 1. Pré-requisitos
- Docker ≥ 24 e Docker Compose v2
- ~8 GB RAM disponível

### 2. Estrutura de diretórios
```
.
├── docker-compose.yml
├── .env                        ← copie de .env.example
├── dags/
│   ├── example_spark_delta_minio.py
│   └── scripts/
│       └── spark_delta_example.py
├── logs/
├── plugins/
├── config/
│   └── airflow.cfg             ← opcional
└── spark/
    ├── conf/
    │   └── spark-defaults.conf
    └── jars/                   ← coloque os JARs aqui (ver abaixo)
```

### 3. Configurar variáveis
```bash
cp .env.example .env
# Edite .env com suas senhas e gere a Fernet key:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 4. Baixar JARs do Spark (Delta + S3A)
```bash
mkdir -p spark/jars
cd spark/jars

# Delta Lake 3.2.0 (compatível com Spark 3.5)
wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar
wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

# Hadoop AWS (S3A para MinIO)
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
```

### 5. Criar diretórios necessários
```bash
mkdir -p logs plugins config dags/scripts spark/conf spark/jars
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```

### 6. Subir o stack
```bash
# Inicialização (primeira vez)
docker compose up airflow-init -d
docker compose logs -f airflow-init   # aguarde "Initialized successfully"

# Subir todos os serviços
docker compose up -d

# Acompanhar logs
docker compose logs -f
```

### 7. Verificar saúde dos serviços
```bash
docker compose ps
```

---

## 🔗 Conexão Airflow → Spark

No Airflow UI (`http://localhost:8080`), vá em **Admin → Connections** e crie:

| Campo       | Valor                          |
|-------------|--------------------------------|
| Conn Id     | `spark_default`                |
| Conn Type   | `Spark`                        |
| Host        | `spark://spark-master`         |
| Port        | `7077`                         |

---

## 🔗 Conectar Metabase ao PostgreSQL do Airflow

No setup inicial do Metabase (`http://localhost:3000`):
- **Host:** `postgres-airflow`
- **Port:** `5432`
- **Database:** `airflow`
- **User/Pass:** conforme `.env`

> ⚠️ Use o hostname `postgres-airflow` (nome do serviço Docker) — não `localhost`.

---

## 🗂️ Buckets MinIO criados automaticamente

| Bucket        | Uso                          |
|---------------|------------------------------|
| `delta-lake`  | Tabelas Delta Lake           |
| `spark-logs`  | Logs de eventos do Spark     |
| `airflow-logs`| Logs do Airflow              |

---

## 🔄 Comandos úteis

```bash
# Parar tudo
docker compose down

# Parar e apagar volumes (⚠️ destrói dados)
docker compose down -v

# Reiniciar só o Airflow Scheduler
docker compose restart airflow-scheduler

# Escalar workers
docker compose up --scale airflow-worker=3 -d

# Ver logs de um serviço específico
docker compose logs -f spark-master
```

---

## ⚠️ Resolução de conflitos de porta

Se alguma porta já estiver em uso na sua máquina, edite o `docker-compose.yml`:

```yaml
# Exemplo: mudar Airflow de 8080 para 8090
ports:
  - "8090:8080"   # host:container
```

| Conflito comum          | Causa                          | Solução sugerida         |
|-------------------------|--------------------------------|--------------------------|
| 5432 ocupada            | PostgreSQL local               | Mudar host para `5434`   |
| 8080 ocupada            | Outro servidor web             | Mudar para `8090`        |
| 9000 ocupada            | Outro S3/objeto store          | Mudar para `9002`        |
| 3000 ocupada            | Grafana ou outro app           | Mudar para `3001`        |
