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
MQTT_DOMAIN = "cws"

mongo_connection_str = f"mongodb://{MONGO_ADDRESS}:{MONGO_PORT}/"
mongo_client = pymongo.MongoClient(mongo_connection_str)

def on_connect(client : mqtt.Client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(f"/{MQTT_DOMAIN}/#")
    else:
        print(f"Failed to connect, return code: {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    path = msg.topic.split("/")

    if (len(path) != 6 or path[1] != MQTT_DOMAIN):
        return;

    top, build_id, room_id, type_id, device_id = path[1:]
    data = {}

    try:
        data["room"] = room_id
        data["type"] = type_id
        data["sensor"] = device_id
        data["payload"] = json.loads(payload)
    except ValueError as e:
        print(repr(e))
        return

    database = mongo_client[top]
    collection = database[build_id]

    print(data)
    collection.insert_one(data)

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(BROKER_ADDRESS, BROKER_PORT, keepalive=60)
mqtt_client.loop_forever()

