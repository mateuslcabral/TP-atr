import psutil
import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import threading
import socket

QOS = 1
BROKER_ADDRESS = "localhost"
BROKER_PORT = 1883

clientId = "sensor-monitor"
client = mqtt.Client(client_id=clientId)

lock = threading.Lock()

def get_cpu_frequency():
    freq_info = psutil.cpu_freq()
    if freq_info:
        return freq_info.current / 1000
    else:
        return None

def get_cpu_usage():
    return psutil.cpu_percent(interval=1)

def read_and_publish_sensor(client, machine_id, sensor_id, data_interval):
    while True:
        timestamp = datetime.utcnow().isoformat() + 'Z'
        if sensor_id == "sensor1":
            value = get_cpu_frequency()
        elif sensor_id == "sensor2":
            value = get_cpu_usage()
        else:
            value = None

        message = {
            "timestamp": timestamp,
            "value": value
        }

        payload = json.dumps(message)
        topic = f"/sensor_monitors/{machine_id}/{sensor_id}"

        with lock:
            result = client.publish(topic, payload, qos=1)
            print(f"Message published - topic: {topic} - message: {payload}")
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"Failed to publish message: {result.rc}")

        time.sleep(data_interval / 1000.0)

def publish_periodic(client, topic_inicial, payload, periodic):
    while True:
        with lock:
            client.publish(topic_inicial, payload, qos=QOS)
            print("Mensagem inicial enviada")
        time.sleep(periodic / 1000.0)

def on_connect(client, userdata, flags, rc):
    print(f"Conectado ao broker com código de resultado {rc}")
    if rc != 0:
        print("Falha ao conectar ao broker.")
        # Implementar lógica de reconexão ou encerramento seguro

def main(data_interval1, data_interval2, periodic):
    client.on_connect = on_connect

    try:
        client.connect(BROKER_ADDRESS)
        client.loop_start()

    except Exception as e:
        print(f"Erro: {e}")
        return

    print("Conectado ao broker")

    machine_id = socket.gethostname()
    sensor_id1 = "sensor1"
    sensor_id2 = "sensor2"

    j_sensor1 = {
        "sensor_id": sensor_id1,
        "data_type": "cpu_frequency",
        "data_interval": data_interval1
    }

    j_sensor2 = {
        "sensor_id": sensor_id2,
        "data_type": "cpu_usage",
        "data_interval": data_interval2
    }

    j_inicial = {
        "machine_id": machine_id,
        "sensors": [j_sensor1, j_sensor2]
    }

    topic_inicial = "/sensor_monitors"
    payload = json.dumps(j_inicial).encode()

    threading.Thread(target=publish_periodic, args=(client, topic_inicial, payload, periodic)).start()
    threading.Thread(target=read_and_publish_sensor, args=(client, machine_id, sensor_id1, data_interval1)).start()
    threading.Thread(target=read_and_publish_sensor, args=(client, machine_id, sensor_id2, data_interval2)).start()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print(f"Uso: {sys.argv[0]} <data_interval1> <data_interval2> <periodic>")
        data_interval1 = 1000
        data_interval2 = 2000
        periodic = 5000
    else:
        data_interval1 = int(sys.argv[1])
        data_interval2 = int(sys.argv[2])
        periodic = int(sys.argv[3])

    main(data_interval1, data_interval2, periodic)
