from pyspark.sql import SparkSession

# Configurações explícitas para garantir conectividade
spark = (
    SparkSession.builder.appName("TesteDocker")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", "spark-master")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.port", "40450")
    .config("spark.blockManager.port", "40451")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", "2")
    .config("spark.cores.max", "4")
    .config("spark.dynamicAllocation.enabled", "false")
    .getOrCreate()
)

# Teste que exige comunicação driver-worker
from pyspark.sql.functions import spark_partition_id

df = spark.range(0, 10000, 1, 4)  # 4 partições
df_with_part = df.withColumn("partition", spark_partition_id())

# Esta operação exige que workers reportem ao driver
print("Partições e contagens:")
df_with_part.groupBy("partition").count().show()

print(f"\nExecutores ativos: {spark.sparkContext.getExecutorMemoryStatus()}")
spark.stop()
