# 🌡️ Proyecto de Observación de Datos en Streaming

### Pipeline IoT end-to-end: Wokwi → MQTT → Kafka → Spark → MinIO

> **El objetivo en una frase:** un sensor de temperatura **simulado** manda sus lecturas
> por internet y nuestro sistema las recoge, las transporta, las procesa **al vuelo** y
> las guarda ordenadas en un almacén de datos — todo en **tiempo real** y sin que nadie
> toque nada.

Desde que el sensor publica una lectura hasta que está guardada como Parquet en el
data lake pasan **menos de 40 segundos**, y el sistema corre solo las 24 horas.

---

## 🗺️ Diagrama de flujo

```
   EL MUNDO EXTERIOR (internet)                    NUESTRA MÁQUINA (Docker)
┌────────────────────────────────────┐   ┌──────────────────────────────────────────┐
│                                    │   │                                          │
│  ┌──────────────────┐              │   │   ┌──────────────────┐                   │
│  │ 1️⃣ WOKWI          │              │   │   │ 3️⃣ BRIDGE         │                   │
│  │ (navegador)       │   publica    │   │   │ (Python)          │                   │
│  │                  │   MQTT       │   │   │                  │                   │
│  │  ESP32 + sensor   │      │       │   │   │ escucha HiveMQ    │                   │
│  │  DHT22 virtual    │      ▼       │   │   │ y lo reenvía ──┐  │                   │
│  │  cada 5 segundos  │  ┌────────┐  │   │   └────────▲───────┼──┘                   │
│  └──────────────────┘  │2️⃣HiveMQ │◄─┼───┼────────────┘       │                       │
│                        │ broker  │  │   │  se suscribe       ▼                       │
│                        │ público │  │   │   ┌──────────────────┐                   │
│                        └────────┘  │   │   │ 4️⃣ KAFKA          │                   │
│                                    │   │   │ topic iot-sensor  │                   │
└────────────────────────────────────┘   │   └────────┬─────────┘                   │
                                          │            │ lee en streaming            │
                                          │            ▼                             │
                                          │   ┌──────────────────┐                   │
                                          │   │ 5️⃣ SPARK          │                   │
                                          │   │ Structured        │                   │
                                          │   │ Streaming         │                   │
                                          │   │ (procesa al vuelo)│                   │
                                          │   └────────┬─────────┘                   │
                                          │            │ escribe Parquet             │
                                          │            ▼                             │
                                          │   ┌──────────────────┐                   │
                                          │   │ 6️⃣ MinIO          │                   │
                                          │   │ bucket raw-data   │                   │
                                          │   │ (data lake)       │                   │
                                          │   └──────────────────┘                   │
                                          └──────────────────────────────────────────┘

  Ventanas para MIRAR el proceso:  Kafka UI (localhost:8080) · MinIO (localhost:9001)
```

---

## 🧩 Cada pieza explicada: qué hace y POR QUÉ está ahí

### 1️⃣ Wokwi — el sensor que no existe
**Qué es:** un simulador de placas electrónicas (ESP32, Arduino) que corre en el
navegador. Nuestro "sensor" es un chip ESP32 con un termómetro DHT22, ambos virtuales.

**Qué hace:** cada 5 segundos lee temperatura y humedad y publica un mensaje JSON:
`{"device":"esp32-wokwi-01","seq":1,"temperature":24.5,"humidity":55.0}`

**¿Por qué simulado y no real?** Para aprender la **arquitectura de datos** da igual
que el termómetro sea de plástico o de píxeles: el mensaje que viaja es idéntico.
Wokwi nos da un dispositivo IoT gratis, sin hardware, y además **interactivo**: clicas
el sensor, mueves el deslizador de temperatura y ves el cambio recorrer todo el
sistema. Si mañana hubiera un ESP32 físico, **no habría que cambiar ni una línea del
resto del pipeline**.

### 2️⃣ HiveMQ — la oficina de correos
**Qué es:** un *broker* MQTT público y gratuito en internet. MQTT es el "idioma"
estándar de los dispositivos IoT: mensajes minúsculos, ideales para aparatos con poca
potencia y batería.

**¿Por qué lo necesitamos?** Por una limitación física: **Wokwi corre en el navegador
y no puede ver tu ordenador** (tu localhost). Hace falta un punto de encuentro neutral
que ambos alcancen: el sensor deja la carta en el buzón público y nosotros la
recogemos desde casa.

### 3️⃣ Bridge — el traductor
**Qué es:** un pequeño programa Python propio ([bridge/mqtt_kafka_bridge.py](bridge/mqtt_kafka_bridge.py)),
el único código de "pegamento" del proyecto.

**Qué hace:** (a) se suscribe al topic de HiveMQ y recibe cada mensaje del sensor;
(b) lo reenvía a Kafka añadiéndole metadatos de trazabilidad (cuándo y por dónde entró).

**¿Por qué hace falta?** MQTT y Kafka no se hablan directamente: MQTT es el idioma de
los **dispositivos**, Kafka el de las **plataformas de datos**. El bridge es el
adaptador. En la industria esta pieza existe siempre (p. ej. como "Kafka Connect
MQTT"); aquí la escribimos a mano para entenderla.

### 4️⃣ Kafka — la cinta transportadora
**Qué es:** una plataforma de mensajería distribuida: una **cinta transportadora
industrial** donde los mensajes avanzan en orden y nada se pierde.

**¿Por qué no conectar el sensor directo a Spark?** Tres motivos (la razón de ser de Kafka):
- **Amortigua picos**: si llegaran 10.000 sensores de golpe, Kafka aguanta y Spark procesa a su ritmo.
- **Desacopla**: si Spark se cae 10 minutos no se pierde ni un dato; al volver sigue donde lo dejó.
- **Multiplica**: mañana podrían leer del mismo topic una alarma, una BD y un dashboard sin tocar el sensor.

**Detalles técnicos:** imagen oficial `apache/kafka` 4.1 en modo **KRaft** (el modo
moderno, sin Zookeeper). El topic `iot-sensor` se crea **explícitamente** (3
particiones) con un contenedor de inicialización — sin depender del auto-create.

### 5️⃣ Spark Structured Streaming — la fábrica que nunca cierra
**Qué es:** el motor de procesamiento más usado del mundo big data, en modo
*streaming*: en vez de procesar un fichero y terminar, **se queda escuchando para
siempre**.

**Qué hace** ([spark/streaming_job.py](spark/streaming_job.py)): cada 30 segundos toma
lo nuevo de Kafka (un *micro-batch*), valida y estructura el JSON (tipos correctos,
descarta mensajes rotos), añade columnas de fecha/hora y lo escribe ordenado en MinIO.

**¿Quién ejecuta el script?** El contenedor `iot-spark`. Su comando de arranque es
**`spark-submit`**, el lanzador oficial de trabajos Spark, que ejecuta nuestro script
(montado por volumen en `/app`). Matiz importante: **PySpark no es "Python procesando
datos"** — el script Python solo *describe* las transformaciones; quien las ejecuta es
el motor de Spark (JVM) dentro del contenedor. Con `--master local[*]` driver y
ejecutores corren en ese mismo contenedor; **el mismo script funcionaría sin cambios
en un clúster real** de cien máquinas.

**¿Por qué Spark y no un simple script?** Tolerancia a fallos con **checkpoint** (si
se reinicia no pierde ni duplica: sabe por dónde iba), escalado horizontal, y es el
estándar de la industria que esta práctica enseña.

### 6️⃣ MinIO — el almacén final
**Qué es:** un data lake compatible con Amazon S3 corriendo en local. El destino del viaje.

**Qué hace:** guarda los datos en **Parquet** (formato columnar y comprimido, el
estándar analítico) **particionados por fecha y hora**
(`raw-data/iot-sensor/fecha=2026-06-11/hora=7/...`), de modo que una consulta tipo
"dame las lecturas de ayer de 7 a 8" lea solo esa carpeta y no todo el histórico.

**¿Por qué un data lake y no una BD normal?** Los datos de sensores crecen sin parar:
un lake es barato, infinito y perfecto para análisis posterior. Y al ser API S3, todo
lo aprendido vale tal cual para la nube real (AWS).

### 🐳 Docker — la caja que lo envuelve todo
Todas las piezas locales (Kafka, bridge, Spark, MinIO, interfaces) viven en
contenedores y se levantan con **un solo comando**. ¿Por qué? **Reproducibilidad**:
funciona igual en cualquier PC, sin instalar nada, sin conflictos de versiones de
Python o Java.

---

## 🚀 Cómo ejecutarlo

### Requisitos
- Docker Desktop corriendo.
- Un navegador (para Wokwi). Nada más.

### 1. Levantar la infraestructura local

```bash
docker compose up -d --build
```

> La primera vez tarda varios minutos: descarga las imágenes y Spark baja los
> conectores de Kafka y S3 (quedan cacheados en un volumen para las siguientes).

### 2. Arrancar el sensor en Wokwi

Sigue [wokwi/README.md](wokwi/README.md): crear un proyecto ESP32 en
[wokwi.com](https://wokwi.com), copiar `sketch.ino` y `diagram.json`, añadir las
librerías `PubSubClient` y `DHT sensor library for ESPx` (Library Manager o
`libraries.txt`) y pulsar ▶.

> ⚠️ El topic MQTT del sketch y el `MQTT_TOPIC` del `.env` deben **coincidir**, y
> conviene personalizarlo: el broker HiveMQ es público y compartido.

### 3. Observar el dato fluir (de aquí el nombre del proyecto 🙂)

| Punto del pipeline | Dónde mirarlo |
|---|---|
| Sensor publicando | Monitor serie de Wokwi |
| Bridge reenviando | `docker logs -f iot-bridge` |
| Mensajes en la cola | **Kafka UI** → http://localhost:8080 → topic `iot-sensor` |
| Spark procesando | `docker logs -f iot-spark` |
| Parquet en el lake | **MinIO** → http://localhost:9001 (admin/admin123) → `raw-data` |

### 4. La demo estrella 🌟

En Wokwi, **clica el sensor DHT22** del diagrama: aparecen deslizadores de temperatura
y humedad. Muévelos y verás los nuevos valores reflejados en el bridge, en Kafka UI y
en el siguiente Parquet **en segundos**. Streaming de verdad, visible.

### Parar todo

```bash
docker compose down        # conserva los datos (volúmenes)
docker compose down -v     # borra también los datos
```

---

## 🛡️ Defensa rápida — preguntas frecuentes

**"¿Por qué tantas piezas para un termómetro?"**
Porque el objetivo no es medir temperatura: es construir la **arquitectura estándar de
ingesta IoT en streaming** que usan las empresas (miles de sensores → broker → cola →
procesado → lake). Con un sensor se aprende; la arquitectura ya está lista para mil.

**"¿Qué significa 'streaming' aquí?"**
Que el dato **nunca espera**: se procesa según llega, no se acumula para procesarse por
la noche (eso sería *batch*). Nuestro Spark usa internamente **micro-batches** de 30 s:
streaming con ficheros de salida de tamaño razonable — el punto medio entre ambos mundos.

**"¿Qué pasa si se cae X?"**
- ¿Se cierra Wokwi? El resto espera tranquilamente; al volver a darle Play, retoma.
- ¿Se cae Spark? Kafka retiene los mensajes; al volver, Spark retoma desde su
  checkpoint **sin perder ni duplicar**.
- ¿Se cae el bridge? Docker lo reinicia solo (`restart: unless-stopped`), y su
  productor es **idempotente** (sin duplicados por reintento).

**"¿Esto escala?"**
Sí, por diseño: el topic ya tiene 3 particiones, y Kafka y Spark están hechos para
repartir la carga entre máquinas. La arquitectura no cambia; solo el tamaño del clúster.

**"¿Y la pega del broker público?"**
HiveMQ público no tiene autenticación: cualquiera podría leer el topic (por eso usamos
un nombre único). En producción: broker propio con TLS y usuarios. Es un compromiso
consciente de práctica educativa — la limitación viene de que Wokwi no ve localhost.

---

## 🔌 Interfaces web

| Servicio | URL | Credenciales |
|---|---|---|
| 📊 Kafka UI | http://localhost:8080 | — |
| 🗄️ MinIO Console | http://localhost:9001 | `admin` / `admin123` |

## ⚙️ Configuración (`.env`)

| Variable | Descripción | Por defecto |
|---|---|---|
| `MQTT_BROKER` / `MQTT_PORT` | Broker MQTT público | `broker.hivemq.com` / `1883` |
| `MQTT_TOPIC` | Topic donde publica Wokwi (¡único por usuario!) | `iabd/<usuario>/iot-sensor` |
| `KAFKA_TOPIC` | Topic de Kafka | `iot-sensor` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | Credenciales MinIO | `admin` / `admin123` |

## 📁 Estructura

```
.
├── docker-compose.yml      # Kafka KRaft + Kafka UI + MinIO + bridge + Spark
├── .env.example            # plantilla de configuración (copiar a .env)
├── wokwi/                  # simulación del sensor (se ejecuta en wokwi.com)
│   ├── sketch.ino          #   ESP32 + DHT22 → MQTT (HiveMQ)
│   ├── diagram.json        #   conexionado del circuito
│   └── libraries.txt       #   PubSubClient + DHT sensor library
├── bridge/                 # puente MQTT → Kafka (Python, contenedorizado)
│   ├── mqtt_kafka_bridge.py
│   └── Dockerfile
└── spark/
    └── streaming_job.py    # job PySpark: Kafka → Parquet en MinIO (s3a)
```

---

## 🎤 La frase de cierre

> *"Hemos montado un pipeline de streaming IoT de extremo a extremo con las piezas
> estándar de la industria: el dispositivo habla MQTT, un bridge lo traduce a Kafka
> que actúa de amortiguador, Spark lo procesa en continuo con garantías de no perder
> ni duplicar, y un data lake S3 lo almacena en formato analítico particionado. Cada
> pieza es sustituible sin tocar las demás, y todo se levanta con un comando."*
