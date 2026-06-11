# Simulación Wokwi — ESP32 + DHT22 → MQTT

Esta carpeta contiene la simulación del sensor IoT. **No se ejecuta en local**:
Wokwi es un simulador online que corre en el navegador.

## Cómo ponerla en marcha

1. Ve a **https://wokwi.com** → *New Project* → **ESP32**.
2. Sustituye el contenido de las pestañas del editor:
   - `sketch.ino` → copia el contenido de [sketch.ino](sketch.ino)
   - `diagram.json` → copia el contenido de [diagram.json](diagram.json)
   - `libraries.txt` → copia el contenido de [libraries.txt](libraries.txt)
3. (Opcional) Cambia el topic MQTT en el sketch (`MQTT_TOPIC`) — debe ser
   **único** y **coincidir** con `MQTT_TOPIC` del `.env` del proyecto.
4. Pulsa **▶ (Play)**. En el monitor serie verás:
   - La conexión a la WiFi simulada (`Wokwi-GUEST`)
   - La conexión al broker MQTT (HiveMQ)
   - Cada 5 s, el JSON publicado: `{"device":"esp32-wokwi-01","seq":1,"temperature":24.5,"humidity":55.0}`
5. **Clica el sensor DHT22** en el diagrama para cambiar la temperatura/humedad
   con los deslizadores y ver cómo cambian los datos en todo el pipeline.

## Qué pasa después

El contenedor `iot-bridge` (en tu máquina) está suscrito al mismo topic en
HiveMQ: recibe cada mensaje y lo publica en el topic `iot-sensor` de Kafka.
Desde ahí, Spark Structured Streaming lo procesa y lo guarda en MinIO.
