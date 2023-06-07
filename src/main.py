import paho.mqtt.client as mqtt
import sys

if __name__ != "__main__":
    exit(0)

if len(sys.argv) != 3:
    print("Args: <broker_address> <broker_port>")
    sys.exit(1)

sensor_type = "temp"
broker_address = sys.argv[1]
broker_port = int(sys.argv[2])

def on_connect(client : mqtt.Client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe("/cws/#")
    else:
        print(f"Failed to connect, return code: {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"From topic '{msg.topic}' received '{payload}'")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker_address, broker_port, keepalive=60)
client.loop_forever()

