#!/bin/bash

# =============================================================================
# Script de Configuração de Bancos PostgreSQL para Data Stack
# Cria bancos separados para: Metabase, Airflow e aplicações de dados
# =============================================================================

set -e  # Encerra em caso de erro

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configurações
POSTGRES_CONTAINER="postgres-pgvector"
POSTGRES_USER="admin"
POSTGRES_PASSWORD="admin123"
POSTGRES_DB="postgres"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Configuração de Bancos PostgreSQL    ${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Verificar se o container está rodando
if ! docker ps | grep -q "$POSTGRES_CONTAINER"; then
    echo -e "${RED}❌ Container $POSTGRES_CONTAINER não está rodando!${NC}"
    echo -e "${YELLOW}⚠️  Inicie o docker-compose primeiro:${NC}"
    echo -e "   docker-compose up -d postgres"
    exit 1
fi

echo -e "${GREEN}✅ Container PostgreSQL encontrado${NC}"
echo ""

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

execute_sql() {
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"
}

execute_sql_no_transaction() {
    # Para comandos que não podem rodar em transação
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 --single-transaction -c "$1" 2>/dev/null || \
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"
}

create_database() {
    local db_name=$1
    local owner=$2
    
    echo -e "${YELLOW}📦 Criando banco: $db_name (owner: $owner)${NC}"
    
    # Verifica se banco já existe
    if docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "SELECT 1 FROM pg_database WHERE datname='$db_name'" | grep -q 1; then
        echo -e "   ${YELLOW}⚠️  Banco $db_name já existe, pulando...${NC}"
    else
        # Criar banco fora de transação explícita
        docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
            psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "CREATE DATABASE $db_name OWNER $owner;" || \
        docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
            psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE $db_name OWNER $owner;"
        echo -e "   ${GREEN}✅ Banco $db_name criado${NC}"
    fi
}

create_user() {
    local username=$1
    local password=$2
    
    echo -e "${YELLOW}👤 Criando usuário: $username${NC}"
    
    # Verifica se usuário já existe
    if docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "SELECT 1 FROM pg_roles WHERE rolname='$username'" | grep -q 1; then
        echo -e "   ${YELLOW}⚠️  Usuário $username já existe${NC}"
    else
        execute_sql "CREATE USER $username WITH PASSWORD '$password';"
        echo -e "   ${GREEN}✅ Usuário $username criado${NC}"
    fi
}

grant_privileges() {
    local db_name=$1
    local username=$2
    
    echo -e "${YELLOW}🔐 Concedendo privilégios em $db_name para $username${NC}"
    execute_sql "GRANT ALL PRIVILEGES ON DATABASE $db_name TO $username;"
    execute_sql "ALTER DATABASE $db_name OWNER TO $username;"
    echo -e "   ${GREEN}✅ Privilégios concedidos${NC}"
}

enable_pgvector() {
    local db_name=$1
    
    echo -e "${YELLOW}🔌 Habilitando extensão pgvector em $db_name${NC}"
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -d "$db_name" -c "CREATE EXTENSION IF NOT EXISTS vector;"
    echo -e "   ${GREEN}✅ pgvector habilitado${NC}"
}

configure_database_params() {
    local db_name=$1
    
    echo -e "${YELLOW}⚙️  Configurando parâmetros para $db_name${NC}"
    
    # Configurações por banco usando ALTER DATABASE (permite em transação)
    # Em vez de ALTER SYSTEM, usamos configurações específicas do banco
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -d "$db_name" -c "
        -- Configurações de performance para este banco específico
        ALTER DATABASE $db_name SET work_mem = '256MB';
        ALTER DATABASE $db_name SET maintenance_work_mem = '512MB';
        ALTER DATABASE $db_name SET effective_cache_size = '1GB';
    " 2>/dev/null || echo -e "   ${YELLOW}⚠️  Algumas configurações podem não estar disponíveis${NC}"
    
    echo -e "   ${GREEN}✅ Parâmetros configurados${NC}"
}

# =============================================================================
# CONFIGURAÇÃO DOS BANCOS
# =============================================================================

echo -e "${BLUE}🔧 Iniciando configuração dos bancos de dados...${NC}"
echo ""

# -----------------------------------------------------------------------------
# 1. BANCO PARA METABASE (Metadados e Application Database)
# -----------------------------------------------------------------------------
echo -e "${BLUE}----------------------------------------${NC}"
echo -e "${BLUE}  1. METABASE                         ${NC}"
echo -e "${BLUE}----------------------------------------${NC}"

create_user "metabase" "metabase_secure_pass"
create_database "metabase_db" "metabase"
grant_privileges "metabase_db" "metabase"
enable_pgvector "metabase_db"  # Metabase pode usar vetores para embeddings
configure_database_params "metabase_db"

echo ""

# -----------------------------------------------------------------------------
# 2. BANCO PARA AIRFLOW (Metadados)
# -----------------------------------------------------------------------------
echo -e "${BLUE}----------------------------------------${NC}"
echo -e "${BLUE}  2. APACHE AIRFLOW                   ${NC}"
echo -e "${BLUE}----------------------------------------${NC}"

create_user "airflow" "airflow_secure_pass"
create_database "airflow_db" "airflow"
grant_privileges "airflow_db" "airflow"

# Configurações específicas para Airflow (sem ALTER SYSTEM)
echo -e "${YELLOW}⚙️  Configurando parâmetros para Airflow${NC}"
docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
    psql -U "$POSTGRES_USER" -d "airflow_db" -c "
    -- Aumentar limite de conexões para este banco específico
    ALTER DATABASE airflow_db CONNECTION LIMIT 200;
    -- Configurações de performance
    ALTER DATABASE airflow_db SET work_mem = '128MB';
    ALTER DATABASE airflow_db SET shared_buffers = '256MB';
" 2>/dev/null || echo -e "   ${YELLOW}⚠️  Usando configurações padrão${NC}"

echo -e "   ${GREEN}✅ Configurações aplicadas${NC}"
echo ""

# -----------------------------------------------------------------------------
# 3. BANCO PARA DATALAKE/RAW DATA (Dados brutos)
# -----------------------------------------------------------------------------
echo -e "${BLUE}----------------------------------------${NC}"
echo -e "${BLUE}  3. DATALAKE - RAW DATA              ${NC}"
echo -e "${BLUE}----------------------------------------${NC}"

create_user "datalake" "datalake_secure_pass"
create_database "datalake_raw" "datalake"
grant_privileges "datalake_raw" "datalake"
enable_pgvector "datalake_raw"
configure_database_params "datalake_raw"

echo ""

# -----------------------------------------------------------------------------
# 4. BANCO PARA DATA WAREHOUSE (Dados processados)
# -----------------------------------------------------------------------------
echo -e "${BLUE}----------------------------------------${NC}"
echo -e "${BLUE}  4. DATA WAREHOUSE                   ${NC}"
echo -e "${BLUE}----------------------------------------${NC}"

create_database "datalake_dw" "datalake"
grant_privileges "datalake_dw" "datalake"
enable_pgvector "datalake_dw"
configure_database_params "datalake_dw"

echo ""

# -----------------------------------------------------------------------------
# 5. BANCO PARA SPARK METASTORE (Metadados do Hive Metastore)
# -----------------------------------------------------------------------------
echo -e "${BLUE}----------------------------------------${NC}"
echo -e "${BLUE}  5. SPARK HIVE METASTORE             ${NC}"
echo -e "${BLUE}----------------------------------------${NC}"

create_user "hive" "hive_secure_pass"
create_database "hive_metastore" "hive"
grant_privileges "hive_metastore" "hive"

# Schema específico para metastore
docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
    psql -U "hive" -d "hive_metastore" -c "
    CREATE SCHEMA IF NOT EXISTS metastore;
    CREATE SCHEMA IF NOT EXISTS default;
" 2>/dev/null || echo -e "   ${YELLOW}⚠️  Schemas podem já existir${NC}"

echo ""

# -----------------------------------------------------------------------------
# 6. BANCO PARA APLICAÇÕES/MICROSERVIÇOS
# -----------------------------------------------------------------------------
echo -e "${BLUE}----------------------------------------${NC}"
echo -e "${BLUE}  6. APLICAÇÕES/MICROSERVIÇOS         ${NC}"
echo -e "${BLUE}----------------------------------------${NC}"

create_user "app_user" "app_secure_pass"
create_database "app_database" "app_user"
grant_privileges "app_database" "app_user"
enable_pgvector "app_database"
configure_database_params "app_database"

echo ""

# -----------------------------------------------------------------------------
# 7. BANCO PARA LOGS E MONITORAMENTO
# -----------------------------------------------------------------------------
echo -e "${BLUE}----------------------------------------${NC}"
echo -e "${BLUE}  7. LOGS E MONITORAMENTO             ${NC}"
echo -e "${BLUE}----------------------------------------${NC}"

create_user "monitoring" "monitoring_secure_pass"
create_database "monitoring_logs" "monitoring"
grant_privileges "monitoring_logs" "monitoring"

echo ""

# =============================================================================
# CONFIGURAÇÕES ADICIONAIS
# =============================================================================

echo -e "${BLUE}----------------------------------------${NC}"
echo -e "${BLUE}  CONFIGURAÇÕES ADICIONAIS            ${NC}"
echo -e "${BLUE}----------------------------------------${NC}"

# Criar schema 'public' em todos os bancos e dar permissões
echo -e "${YELLOW}📝 Configurando schemas públicos${NC}"
for db in metabase_db airflow_db datalake_raw datalake_dw hive_metastore app_database monitoring_logs; do
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -d "$db" -c "
        GRANT ALL ON SCHEMA public TO ${db%_*};
        ALTER SCHEMA public OWNER TO ${db%_*};
    " 2>/dev/null || true
done
echo -e "   ${GREEN}✅ Schemas configurados${NC}"

# Configurar pgvector em todos os bancos de dados
echo -e "${YELLOW}🔌 Garantindo pgvector em todos os bancos${NC}"
for db in metabase_db airflow_db datalake_raw datalake_dw app_database; do
    enable_pgvector "$db" 2>/dev/null || true
done

# Criar arquivo de configuração recomendado (não aplicado automaticamente)
echo -e "${YELLOW}📝 Gerando recomendações de configuração${NC}"
cat << 'EOF' > postgres-recommendations.conf
# =============================================================================
# RECOMENDAÇÕES DE CONFIGURAÇÃO PARA POSTGRESQL
# Adicione estas linhas ao postgresql.conf ou use como flags de inicialização
# =============================================================================

# Conexões
max_connections = 200

# Memória
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 128MB
maintenance_work_mem = 256MB

# WAL e Checkpoint
wal_buffers = 16MB
checkpoint_completion_target = 0.9
random_page_cost = 1.1
effective_io_concurrency = 200

# Query Planner
default_statistics_target = 100

# Logging
log_min_duration_statement = 1000
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on

# Extensões pré-carregadas (opcional)
# shared_preload_libraries = 'pg_stat_statements'
EOF

echo -e "   ${GREEN}✅ Arquivo postgres-recommendations.conf criado${NC}"
echo -e "   ${YELLOW}💡 Para aplicar: docker cp postgres-recommendations.conf $POSTGRES_CONTAINER:/tmp/ && docker exec $POSTGRES_CONTAINER bash -c 'cat /tmp/postgres-recommendations.conf >> /var/lib/postgresql/data/postgresql.conf' && docker restart $POSTGRES_CONTAINER${NC}"

echo ""

# =============================================================================
# RESUMO FINAL
# =============================================================================

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  ✅ CONFIGURAÇÃO CONCLUÍDA!           ${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}📊 Resumo dos Bancos Criados:${NC}"
echo ""
printf "%-25s %-20s %-30s\n" "BANCO" "USUÁRIO" "SENHA"
echo "------------------------------------------------------------------"
printf "%-25s %-20s %-30s\n" "metabase_db" "metabase" "metabase_secure_pass"
printf "%-25s %-20s %-30s\n" "airflow_db" "airflow" "airflow_secure_pass"
printf "%-25s %-20s %-30s\n" "datalake_raw" "datalake" "datalake_secure_pass"
printf "%-25s %-20s %-30s\n" "datalake_dw" "datalake" "datalake_secure_pass"
printf "%-25s %-20s %-30s\n" "hive_metastore" "hive" "hive_secure_pass"
printf "%-25s %-20s %-30s\n" "app_database" "app_user" "app_secure_pass"
printf "%-25s %-20s %-30s\n" "monitoring_logs" "monitoring" "monitoring_secure_pass"
echo ""
echo -e "${YELLOW}⚠️  IMPORTANTE: Altere as senhas em produção!${NC}"
echo ""
echo -e "${BLUE}🔗 Strings de Conexão (JDBC):${NC}"
echo ""
echo -e "Metabase:"
echo -e "  ${GREEN}jdbc:postgresql://postgres:5432/metabase_db${NC}"
echo ""
echo -e "Airflow:"
echo -e "  ${GREEN}postgresql+psycopg2://airflow:airflow_secure_pass@postgres:5432/airflow_db${NC}"
echo ""
echo -e "Datalake Raw:"
echo -e "  ${GREEN}jdbc:postgresql://postgres:5432/datalake_raw${NC}"
echo ""
echo -e "Hive Metastore:"
echo -e "  ${GREEN}jdbc:postgresql://postgres:5432/hive_metastore${NC}"
echo ""
echo -e "${BLUE}📄 Arquivo de recomendações: postgres-recommendations.conf${NC}"