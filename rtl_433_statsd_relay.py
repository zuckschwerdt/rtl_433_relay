#!/usr/bin/env python

# Statsd monitoring relay for rtl_433.
# Uses Python statsd Network plugin, s.a. https://github.com/jsocol/pystatsd
# pip install pystatsd
# -or- get https://github.com/jsocol/pystatsd/raw/master/statsd/client.py
# (included as statsd.py for convenience)

from __future__ import print_function

import socket
import json
from statsd import StatsClient

UDP_IP = "127.0.0.1"
UDP_PORT = 1433
STATSD_HOST = "127.0.0.1"
STATSD_PORT = 8125
STATSD_PREFIX = "rtlsdr"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))


def sanitize(text):
    return text.replace(" ", "_")


def parse_syslog(line):
    """Try to parse syslog line with JSON payload or RAW JSON."""
    if line.startswith("<"):
        # fields should be "<PRI>VER", timestamp, hostname, command, pid, mid, sdata, payload
        fields = line.split(None, 7)
        line = fields[-1]
    return json.loads(line)


def rtl_433_probe():
    statsd = StatsClient(host=STATSD_HOST,
                         port=STATSD_PORT,
                         prefix=STATSD_PREFIX)

    while True:
        line, addr = sock.recvfrom(1024)

        try:
            data = parse_syslog(line)

            label = sanitize(data["model"])
            if "channel" in data:
                label += ".CH" + str(data["channel"])

            if "battery" in data:
                if data["battery"] == "OK":
                    statsd.gauge(label + '.battery', 1)
                else:
                    statsd.gauge(label + '.battery', 0)

            if "humidity" in data:
                statsd.gauge(label + '.humidity', data["humidity"])

            statsd.gauge(label + '.temperature', data["temperature_C"])

        except KeyError:
            pass

        except ValueError:
            pass


if __name__ == "__main__":
    rtl_433_probe()
