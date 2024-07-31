import paho.mqtt.client as mqtt
import json
import threading
from influxdb import InfluxDBClient
from datetime import datetime, timezone

QOS = 1
subscribed_topics = set()
last_timestamps = {}
sensor_data = {}
client = mqtt.Client()

# Função de callback para quando o cliente se conecta ao broker
def on_connect(client, userdata, flags, rc):
    print(f"Conectado ao broker com código de resultado {rc}")
    if rc == 0:
        client.subscribe("/sensor_monitors")
    else:
        print(f"Failed to connect, return code {rc}\n")

# Função de callback para quando uma mensagem chega
def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode("utf-8")
    print(f"Mensagem recebida no tópico {topic}: {payload}")
    if topic == "/sensor_monitors":
        handle_initial_message(payload)
    else:
        handle_sensor_data_message(client, userdata, msg)

def handle_initial_message(payload):
    try:
        data = json.loads(payload)
        machine_id = data["machine_id"]
        sensors = data["sensors"]
        
        for sensor in sensors:
            sensor_id = sensor["sensor_id"]
            data_interval = sensor["data_interval"]
            sensor_data[(machine_id, sensor_id)] = {"data_interval": data_interval}
            
            topic = f"/sensor_monitors/{machine_id}/{sensor_id}"
            if topic not in subscribed_topics:
                client.subscribe(topic)
                subscribed_topics.add(topic)
                print(f"Subscribed to topic: {topic} e {machine_id}")
    except Exception as e:
        print(f"Error handling sensor monitors message: {e}")

def handle_sensor_data_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)

        machine_id = msg.topic.split('/')[2]
        sensor_id = msg.topic.split('/')[3]
        timestamp_str = data['timestamp']
        value = data['value']

        last_timestamps[(machine_id, sensor_id)] = timestamp_str
        sensor_data[(machine_id, sensor_id)]["value"] = value

        print(f"Sensor data received - Machine ID: {machine_id}, Sensor ID: {sensor_id}, Timestamp: {timestamp_str}, Value: {value}")

        json_body = [
            {
                "measurement": f"{machine_id}.{sensor_id}",
                "tags": {
                    "machine_id": machine_id,
                    "sensor_id": sensor_id
                },
                "time": timestamp_str,
                "fields": {
                    "value": value
                }
            }
        ]
        result = influx_client.write_points(json_body)
        print(f"Data written to InfluxDB - Result: {result}, Measurement: {machine_id}.{sensor_id}, Value: {value}")
    except Exception as e:
        print(f"Error handling sensor data message: {e}")

def check_for_inactivity(machine_id, sensor_id):
    current_time = datetime.now(timezone.utc)
    last_time_str = last_timestamps.get((machine_id, sensor_id))
    
    if last_time_str:
        last_time = datetime.fromisoformat(last_time_str.replace("Z", "+00:00"))
        time_diff = (current_time - last_time).total_seconds()
        
        if time_diff > 5:
            print(f"Alarme gerado - Machine ID: {machine_id}, Sensor ID: {sensor_id}, Tipo: inactive")
            generate_alarm(machine_id, sensor_id, "inactive")

def generate_alarm(machine_id, sensor_id, alarm_type):
    json_body = [
        {
            "measurement": f"{machine_id}.alarms.{alarm_type}",
            "tags": {
                "machine_id": machine_id,
                "sensor_id": sensor_id
            },
            "time": datetime.now(timezone.utc).isoformat(),
            "fields": {
                "value": 1
            }
        }
    ]
    result = influx_client.write_points(json_body)
    print(f"Alarme persistido - Result: {result}, Machine ID: {machine_id}, Sensor ID: {sensor_id}, Tipo: {alarm_type}")

# Conexão com o InfluxDB
try:
    influx_client = InfluxDBClient('localhost', 8086, 'root', 'root', 'sensor_data')
    influx_client.ping()
    print("Conexão com InfluxDB estabelecida com sucesso.")
except Exception as e:
    print(f"Erro ao conectar ao InfluxDB: {e}")

client.on_connect = on_connect
client.on_message = on_message

# Função para verificar a inatividade de todos os sensores periodicamente
def periodic_inactivity_check():
    for (machine_id, sensor_id) in last_timestamps.keys():
        check_for_inactivity(machine_id, sensor_id)
    
    # Agendar a próxima verificação em 5 segundos
    threading.Timer(5, periodic_inactivity_check).start()

# Iniciar a verificação periódica
periodic_inactivity_check()

def main():
    try:
        client.connect('localhost')  # Certifique-se de que 'localhost' é o endereço correto do broker
        client.loop_forever()
    except Exception as e:
        print(f"Erro ao conectar ao broker: {e}")

if __name__ == "__main__":
    main()
