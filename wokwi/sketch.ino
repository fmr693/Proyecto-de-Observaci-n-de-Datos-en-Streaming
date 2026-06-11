/*
============================================
SENSOR IoT SIMULADO — ESP32 + DHT22 → MQTT
Práctica IoT: Wokwi → Kafka → Spark → MinIO
============================================

Este sketch corre en wokwi.com (simulador de ESP32 en el navegador):
  1. Se conecta a la WiFi del simulador (Wokwi-GUEST)
  2. Lee temperatura y humedad del sensor DHT22 (virtual)
  3. Publica un JSON por MQTT al broker público HiveMQ cada 5 segundos

El "bridge" local se suscribe al mismo topic y reenvía los mensajes a Kafka.

⚠️ IMPORTANTE: el topic MQTT debe ser ÚNICO (el broker es público y compartido)
   y debe coincidir con MQTT_TOPIC del archivo .env del proyecto.

Basado en el patrón clásico de la comunidad Wokwi (ESP32 + DHT22 + PubSubClient).
*/

#include <WiFi.h>
#include <PubSubClient.h>
#include "DHTesp.h"

// --- Configuración ---
const char* WIFI_SSID   = "Wokwi-GUEST";   // WiFi del simulador (sin contraseña)
const char* WIFI_PASS   = "";
const char* MQTT_BROKER = "broker.hivemq.com";
const int   MQTT_PORT   = 1883;
const char* MQTT_TOPIC  = "iabd/fmr693/iot-sensor";   // ← el mismo que en .env
const char* DEVICE_ID   = "esp32-wokwi-01";

const int DHT_PIN = 15;            // pin de datos del DHT22
const unsigned long INTERVALO_MS = 5000;   // publicar cada 5 s

// --- Objetos globales ---
WiFiClient espClient;
PubSubClient mqtt(espClient);
DHTesp dht;
unsigned long ultimoEnvio = 0;
long contador = 0;

void conectarWiFi() {
  Serial.print("Conectando a WiFi");
  WiFi.begin(WIFI_SSID, WIFI_PASS, 6);
  while (WiFi.status() != WL_CONNECTED) {
    delay(300);
    Serial.print(".");
  }
  Serial.println(" ✅ conectado. IP: " + WiFi.localIP().toString());
}

void conectarMQTT() {
  while (!mqtt.connected()) {
    Serial.print("Conectando a MQTT (HiveMQ)...");
    // Client ID único para no chocar con otros usuarios del broker público
    String clientId = String(DEVICE_ID) + "-" + String(random(0xffff), HEX);
    if (mqtt.connect(clientId.c_str())) {
      Serial.println(" ✅ conectado");
    } else {
      Serial.print(" ❌ rc=");
      Serial.print(mqtt.state());
      Serial.println(" — reintento en 2s");
      delay(2000);
    }
  }
}

void setup() {
  Serial.begin(115200);
  Serial.println("\n🌡️  SENSOR IoT SIMULADO — ESP32 + DHT22");

  dht.setup(DHT_PIN, DHTesp::DHT22);
  conectarWiFi();
  mqtt.setServer(MQTT_BROKER, MQTT_PORT);
  conectarMQTT();
}

void loop() {
  if (!mqtt.connected()) conectarMQTT();
  mqtt.loop();

  unsigned long ahora = millis();
  if (ahora - ultimoEnvio >= INTERVALO_MS) {
    ultimoEnvio = ahora;
    contador++;

    // Leer el sensor (en Wokwi puedes cambiar los valores clicando el DHT22)
    TempAndHumidity datos = dht.getTempAndHumidity();

    // Construir el JSON manualmente (ligero, sin librerías extra)
    String payload = "{";
    payload += "\"device\":\"" + String(DEVICE_ID) + "\",";
    payload += "\"seq\":" + String(contador) + ",";
    payload += "\"temperature\":" + String(datos.temperature, 1) + ",";
    payload += "\"humidity\":" + String(datos.humidity, 1);
    payload += "}";

    bool ok = mqtt.publish(MQTT_TOPIC, payload.c_str());
    Serial.println((ok ? "📤 " : "❌ fallo: ") + payload);
  }
}
