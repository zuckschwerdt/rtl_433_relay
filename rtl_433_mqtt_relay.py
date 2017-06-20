#!/usr/bin/env python

"""MQTT monitoring relay for rtl_433 communication."""

# PEP 3143 - Standard daemon process library
# (use Python 3.x or pip install python-daemon)
# import daemon

# needs:
# https://pypi.python.org/pypi/paho-mqtt

from __future__ import print_function
from __future__ import with_statement

import socket
import json
import paho.mqtt.client as mqtt

UDP_IP = "127.0.0.1"
UDP_PORT = 1433

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_PREFIX = "sensor/rtl_433"


def mqtt_connect(client, userdata, flags, rc):
    """Log MQTT connects."""
    print("MQTT connected: " + mqtt.connack_string(rc))


def mqtt_disconnect(client, userdata, rc):
    """Log MQTT disconnects."""
    print("MQTT disconnected: " + mqtt.connack_string(rc))


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
# allow multiple sockets to use the same PORT number
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
sock.bind((UDP_IP, UDP_PORT))


def sanitize(text):
    """Sanitize a name for Graphite/MQTT use."""
    return (text
            .replace(" ", "_")
            .replace("/", "_")
            .replace(".", "_")
            .replace("&", ""))


def publish_sensor_to_mqtt(mqttc, data, line):
    """Publish rtl_433 sensor data to MQTT."""
    path = MQTT_PREFIX
    if "model" in data:
        path += "/" + sanitize(data["model"])
    if "channel" in data:
        path += "/" + str(data["channel"])
    elif "id" in data:
        path += "/" + str(data["id"])

    if "battery" in data:
        if data["battery"] == "OK":
            pass
        else:
            mqttc.publish(path + "/battery", str(data["battery"]))

    if "humidity" in data:
        mqttc.publish(path + "/humidity", data["humidity"])

    if "temperature_C" in data:
        mqttc.publish(path + "/temperature", data["temperature_C"])

    if "depth" in data:
        mqttc.publish(path + "/depth", data["depth"])

    mqttc.publish(path, line)


def rtl_433_probe():
    """Run a rtl_433 UDP listener."""
    mqttc = mqtt.Client()
    mqttc.on_connect = mqtt_connect
    mqttc.on_disconnect = mqtt_disconnect
    mqttc.connect_async(MQTT_HOST, MQTT_PORT, 60)
    mqttc.loop_start()

    while True:
        line, addr = sock.recvfrom(1024)
        try:
            data = json.loads(line)
            publish_sensor_to_mqtt(mqttc, data, line)

        except ValueError:
            pass


def run():
    """Run main or daemon."""
    # with daemon.DaemonContext(files_preserve=[sock]):
    #  detach_process=True
    #  uid
    #  gid
    #  working_directory
    rtl_433_probe()


if __name__ == "__main__":
    run()