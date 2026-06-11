"""
============================================
BRIDGE: MQTT (HiveMQ) → KAFKA
Práctica IoT — Wokwi → Kafka → Spark → MinIO
============================================

El ESP32 simulado en Wokwi corre en el navegador y NO puede ver tu localhost,
así que publica a un broker MQTT público (HiveMQ). Este bridge cierra el hueco:

  1. Se suscribe al topic MQTT donde publica el sensor de Wokwi
  2. Por cada mensaje recibido, lo enriquece con metadatos de ingesta
  3. Lo publica en el topic 'iot-sensor' de Kafka (clave = id del dispositivo)

Desde Kafka, Spark Structured Streaming lo recoge y lo persiste en MinIO.

Uso:  python mqtt_kafka_bridge.py   (o vía Docker: servicio 'bridge')
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from dotenv import load_dotenv

# ============================================
# CONFIGURACIÓN
# ============================================

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# --- MQTT (origen) ---
MQTT_BROKER = os.getenv("MQTT_BROKER", "broker.hivemq.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iabd/fmr693/iot-sensor")

# --- Kafka (destino) ---
KAFKA_BROKER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", f"localhost:{os.getenv('KAFKA_PORT', '29092')}"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-sensor")

# ============================================
# KAFKA PRODUCER
# ============================================

def delivery_callback(err, msg):
    if err:
        log.error(f"  ❌ Error al entregar en Kafka: {err}")


def crear_productor_kafka() -> Producer:
    config = {
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
        "enable.idempotence": True,   # evita duplicados por reintento
        "client.id": "mqtt-kafka-bridge",
    }
    log.info(f"🔌 Conectando a Kafka en {KAFKA_BROKER}...")
    producer = Producer(config)
    metadata = producer.list_topics(timeout=15)
    if KAFKA_TOPIC in metadata.topics:
        log.info(f"✅ Kafka OK. Topic '{KAFKA_TOPIC}' encontrado")
    else:
        log.warning(f"⚠️  Topic '{KAFKA_TOPIC}' no existe aún (lo creará kafka-init)")
    return producer


producer = crear_productor_kafka()
contador = {"recibidos": 0, "publicados": 0}

# ============================================
# CALLBACKS MQTT (API v2 de paho-mqtt)
# ============================================

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        log.info(f"✅ Conectado a MQTT {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC, qos=1)
        log.info(f"👂 Suscrito a '{MQTT_TOPIC}' — esperando datos del sensor Wokwi...")
    else:
        log.error(f"❌ Conexión MQTT rechazada: {reason_code}")


def on_message(client, userdata, msg):
    """Cada mensaje MQTT del sensor → enriquecer → publicar en Kafka."""
    contador["recibidos"] += 1
    try:
        dato = json.loads(msg.payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        log.warning(f"⚠️  Mensaje no-JSON descartado: {e} | payload={msg.payload[:80]!r}")
        return

    # Enriquecer con metadatos de ingesta (trazabilidad)
    dato["_ingest"] = {
        "mqtt_topic": msg.topic,
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "bridge": "mqtt-kafka-bridge",
    }

    clave = str(dato.get("device", "desconocido"))
    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            key=clave,
            value=json.dumps(dato, ensure_ascii=False).encode("utf-8"),
            callback=delivery_callback,
        )
        producer.poll(0)  # atiende callbacks pendientes
        contador["publicados"] += 1
        log.info(
            f"📤 #{contador['publicados']} {clave}: "
            f"T={dato.get('temperature')}°C H={dato.get('humidity')}% → Kafka '{KAFKA_TOPIC}'"
        )
    except Exception as e:
        log.error(f"❌ Error publicando en Kafka: {e}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    log.warning(f"🔌 Desconectado de MQTT (rc={reason_code}). Paho reintentará solo...")


# ============================================
# PARADA LIMPIA
# ============================================

def _parar(signum, frame):
    log.info("🛑 Parando bridge...")
    producer.flush(timeout=10)
    log.info(f"📊 Total: {contador['recibidos']} recibidos, {contador['publicados']} publicados")
    sys.exit(0)


signal.signal(signal.SIGINT, _parar)
signal.signal(signal.SIGTERM, _parar)

# ============================================
# MAIN
# ============================================

def main():
    log.info("=" * 50)
    log.info("🌉 BRIDGE MQTT → KAFKA INICIADO")
    log.info(f"   MQTT:  {MQTT_BROKER}:{MQTT_PORT}  topic '{MQTT_TOPIC}'")
    log.info(f"   Kafka: {KAFKA_BROKER}  topic '{KAFKA_TOPIC}'")
    log.info("=" * 50)

    cliente = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"bridge-{int(time.time())}",  # id único en el broker público
    )
    cliente.on_connect = on_connect
    cliente.on_message = on_message
    cliente.on_disconnect = on_disconnect
    cliente.reconnect_delay_set(min_delay=1, max_delay=30)

    cliente.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    cliente.loop_forever()   # bucle infinito con reconexión automática


if __name__ == "__main__":
    main()
