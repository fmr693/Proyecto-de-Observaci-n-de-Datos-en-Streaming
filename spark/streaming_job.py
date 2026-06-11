"""
============================================
SPARK STRUCTURED STREAMING: KAFKA → MinIO
Práctica IoT — Wokwi → Kafka → Spark → MinIO
============================================

Job de streaming continuo:
  1. Se suscribe al topic 'iot-sensor' de Kafka
  2. Parsea el JSON del sensor (device, seq, temperature, humidity)
  3. Añade el timestamp del evento y columnas de partición (fecha/hora)
  4. Escribe en MinIO (bucket 'raw-data') en formato Parquet, en micro-batches

El checkpoint (también en MinIO) permite parar/reiniciar el job sin perder
ni duplicar datos (exactly-once sobre el sink de ficheros).

Se lanza con spark-submit (ver servicio 'spark' del docker-compose), con los
paquetes:  spark-sql-kafka-0-10  (conector Kafka)  y  hadoop-aws  (s3a/MinIO).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# ============================================
# CONFIGURACIÓN (desde variables de entorno)
# ============================================

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-sensor")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "admin123")

RUTA_SALIDA = "s3a://raw-data/iot-sensor"
RUTA_CHECKPOINT = "s3a://raw-data/_checkpoints/iot-sensor"
TRIGGER = "30 seconds"   # cada cuánto se materializa un micro-batch

# ============================================
# SESIÓN SPARK (configurada para MinIO vía s3a)
# ============================================

spark = (
    SparkSession.builder
    .appName("iot-kafka-to-minio")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")          # MinIO usa rutas, no subdominios
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")    # http en local
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions", "4")                      # volumen pequeño
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"🚀 Spark Streaming iniciado", flush=True)
print(f"   Kafka:  {KAFKA_BOOTSTRAP}  topic '{KAFKA_TOPIC}'", flush=True)
print(f"   Salida: {RUTA_SALIDA}  (trigger: {TRIGGER})", flush=True)

# ============================================
# ESQUEMA DEL MENSAJE DEL SENSOR
# (lo que publica el ESP32 de Wokwi + lo que añade el bridge)
# ============================================

esquema = StructType([
    StructField("device", StringType()),
    StructField("seq", LongType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("_ingest", StructType([
        StructField("mqtt_topic", StringType()),
        StructField("ingest_ts", StringType()),
        StructField("bridge", StringType()),
    ])),
])

# ============================================
# 1) LEER de Kafka (stream)
# ============================================

crudo = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")   # al arrancar, procesa lo pendiente
    .load()
)

# ============================================
# 2) TRANSFORMAR: parsear JSON y añadir columnas de tiempo/partición
# ============================================

sensor = (
    crudo
    .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("timestamp").alias("kafka_ts"),          # cuándo entró en Kafka
        F.from_json(F.col("value").cast("string"), esquema).alias("d"),
    )
    .select(
        "kafka_key",
        "kafka_ts",
        "d.device",
        "d.seq",
        "d.temperature",
        "d.humidity",
        F.col("d._ingest.ingest_ts").cast("timestamp").alias("ingest_ts"),
    )
    .filter(F.col("device").isNotNull())               # descarta mensajes malformados
    .withColumn("fecha", F.to_date("kafka_ts"))        # columnas de partición
    .withColumn("hora", F.hour("kafka_ts"))
)

# ============================================
# 3) ESCRIBIR en MinIO (Parquet particionado, streaming continuo)
# ============================================

query = (
    sensor.writeStream
    .format("parquet")
    .option("path", RUTA_SALIDA)
    .option("checkpointLocation", RUTA_CHECKPOINT)
    .partitionBy("fecha", "hora")
    .trigger(processingTime=TRIGGER)
    .outputMode("append")
    .start()
)

print("👂 Escuchando Kafka... (Ctrl+C para parar)", flush=True)
query.awaitTermination()
