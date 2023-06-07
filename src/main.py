import paho.mqtt.client as mqtt
import sys
from collections import namedtuple
import json
import logging
import pymongo

MQTT_DOMAIN = "cws"


Socket = namedtuple("Socket", "address port")
InputArgs = namedtuple("InputArgs", ["broker_socket", "mongo_socket"])


mongo_client: pymongo.MongoClient


def process_arguments() -> InputArgs:
    try:
        broker_address, broker_port = sys.argv[1].split(":")
        mongo_address, mongo_port = sys.argv[2].split(":")
    except ValueError:
        raise ValueError(f"Look in process_argument() for valid argument passing")

    return InputArgs(
        broker_socket=Socket(address=broker_address, port=int(broker_port)),
        mongo_socket=Socket(address=mongo_address, port=int(mongo_port)),
    )


def mongo_get_client(address, port):
    mongo_connection_str = f"mongodb://{address}:{port}/"
    mongo_client = pymongo.MongoClient(mongo_connection_str)
    return mongo_client


def mqtt_on_connect(client: mqtt.Client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(f"/{MQTT_DOMAIN}/#")
    else:
        print(f"Failed to connect, return code: {rc}")


def mqtt_on_message(client, userdata, msg):
    payload = msg.payload.decode()
    path = msg.topic.split("/")

    if len(path) != 6 or path[1] != MQTT_DOMAIN:
        return

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

    collection.insert_one(data)
    logging.info(data)


def mqtt_connect_loop_forever(address: str, port: int) -> mqtt.Client:
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = mqtt_on_connect
    mqtt_client.on_message = mqtt_on_message

    mqtt_client.connect(address, port, keepalive=60)
    mqtt_client.loop_forever()
    return mqtt_client


if __name__ == "__main__":
    args = process_arguments()

    broker_socket: Socket = args.broker_socket
    mongo_socket: Socket = args.mongo_socket

    mongo_client = mongo_get_client(mongo_socket.address, mongo_socket.port)

    mqtt_connect_loop_forever(broker_socket.address, broker_socket.port)
