import paho.mqtt.client as mqtt
import sys
import json
import pymongo

if __name__ != "__main__":
    exit(0)

if len(sys.argv) != 3:
    print("Args: <broker_address>:<port> <mongo_address>:<port>")
    sys.exit(1)

BROKER_ADDRESS = sys.argv[1].split(":")[0]
BROKER_PORT    = int(sys.argv[1].split(":")[1])
MONGO_ADDRESS  = sys.argv[2].split(":")[0]
MONGO_PORT = int(sys.argv[2].split(":")[1])

mongo_connection_str = f"mongodb://{MONGO_ADDRESS}:{MONGO_PORT}/"
mongo_client = pymongo.MongoClient(mongo_connection_str)

def on_connect(client : mqtt.Client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe("/cws/#")
    else:
        print(f"Failed to connect, return code: {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()

    top, build_id, room, src_type, sensor = msg.topic.split("/")[1:]
    data = json.loads(payload)
    data["room"] = room
    data["type"] = src_type
    data["sensor"] = sensor

    database = mongo_client[top]
    collection = database[build_id]

    collection.insert_one(data)

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(BROKER_ADDRESS, BROKER_PORT, keepalive=60)
mqtt_client.loop_forever()

