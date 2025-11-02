#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sparkplug B Edge Node - System Monitor
--------------------------------------
Dette skriptet publiserer system- og maskin-metrics som en Sparkplug B-kompatibel Edge Node
til HiveMQ Cloud. Det kan brukes sammen med Ignition MQTT Engine.

Publiserer:
  • NBIRTH - node-fødselsmelding (metadata)
  • DBIRTH - device-fødselsmelding (system-tilstand)
  • DDATA  - løpende data- og statusoppdateringer
Håndterer også innkommende DCMD-kommandoer fra SCADA.
"""

# Import standard and external Python libraries
import os  # Used for file operations like checking if a file exists
import time  # Used for waiting and time calculations
import socket  # To get the system's hostname
import ssl  # For secure communication using TLS/SSL
import uuid  # To generate unique identifiers for devices
import psutil  # To access system-level metrics like CPU, memory, etc.
import paho.mqtt.client as mqtt  # MQTT client for connecting to HiveMQ Cloud
from datetime import datetime, timezone # To get current timestamps in a readable format
from sparkplug_b import sparkplug_b_pb2 as spb  # Sparkplug B protocol buffers for message encoding

# Configuration for connecting to the MQTT broker (HiveMQ Cloud)
# https://www.mqtt-dashboard.com/
BROKER = "eu.hivemq.cloud"
PORT = 8883  # Secure port with TLS
USERNAME = "cncoze"
PASSWORD = "passwd"
GROUP_ID = "cncoze"
NODE_ID = socket.gethostname()
DEVICE_ID = "system_monitor"

TOPIC_NBIRTH = f"spBv1.0/{GROUP_ID}/NBIRTH/{NODE_ID}"
TOPIC_DBIRTH = f"spBv1.0/{GROUP_ID}/DBIRTH/{NODE_ID}/{DEVICE_ID}"
TOPIC_DDATA  = f"spBv1.0/{GROUP_ID}/DDATA/{NODE_ID}/{DEVICE_ID}"
TOPIC_DCMD   = f"spBv1.0/{GROUP_ID}/DCMD/{NODE_ID}/{DEVICE_ID}"

# -------------------- 📑 TYPE-KODER --------------------
INT8, INT16, INT32, INT64 = 1, 2, 3, 4
UINT8, UINT16, UINT32, UINT64 = 5, 6, 7, 8
FLOAT, DOUBLE, BOOLEAN, STRING, DATETIME = 9, 10, 11, 12, 13

# -------------------- 🧠 GLOBALE VARIABLER --------------------
seq = 0
connected = False
first = True
previous_metrics = {}

COMMAND_FILE = "command_flag.txt"
if os.path.isfile(COMMAND_FILE):
    os.remove(COMMAND_FILE)

# -------------------- 🔠 ALIAS MAP --------------------
alias_map = {
    "cpu_percent": 0,
    "mem_used_mb": 1,
    "mem_total_mb": 2,
    "disk_used_gb": 3,
    "disk_total_gb": 4,
    "fan_speed": 5,
    "cpu_temp": 6,
    "bytes_sent": 7,
    "bytes_recv": 8,
    "timestamp": 9,
    "online": 10,
    "node": 11,
    "device": 12,
    "battery_percent": 13,
}

# -------------------- 📦 BUILD PAYLOAD --------------------
def build_payload(metrics: dict, is_birth=False) -> bytes:
    global seq
    payload = spb.Payload()
    payload.timestamp = int(time.time() * 1000)
    payload.seq = seq
    seq = (seq + 1) % 256

    if is_birth:
        payload.uuid = str(uuid.uuid4())

    for _, (name, datatype, value) in metrics.items():
        metric = payload.metrics.add()
        metric.alias = alias_map.get(name, 255)
        metric.name = name
        metric.datatype = datatype
        metric.timestamp = payload.timestamp

        if datatype in (INT8, INT16, INT32, UINT8, UINT16, UINT32):
            metric.int_value = int(value)
        elif datatype in (INT64, UINT64):
            metric.long_value = int(value)
        elif datatype == FLOAT:
            metric.float_value = float(value)
        elif datatype == DOUBLE:
            metric.double_value = float(value)
        elif datatype == STRING:
            metric.string_value = str(value)
        elif datatype == BOOLEAN:
            metric.boolean_value = bool(value)

    return payload.SerializeToString()

# -------------------- 🧮 SYSTEM-METRICS --------------------
def read_metrics() -> dict:
    global previous_metrics
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()
    temps = psutil.sensors_temperatures()
    fans = psutil.sensors_fans()
    battery = psutil.sensors_battery()

    cpu_temp = max((entry.current for lst in temps.values() for entry in lst), default=0.0)
    fan_speed = next((dev[0].current for dev in fans.values() if dev), 0)

    metrics = {
        "cpu_percent": ("cpu_percent", DOUBLE, psutil.cpu_percent()),
        "mem_used_mb": ("mem_used_mb", INT64, mem.used // (1024 * 1024)),
        "mem_total_mb": ("mem_total_mb", INT64, mem.total // (1024 * 1024)),
        "disk_used_gb": ("disk_used_gb", INT64, disk.used // (1024 ** 3)),
        "disk_total_gb": ("disk_total_gb", INT64, disk.total // (1024 ** 3)),
        "fan_speed": ("fan_speed", INT64, fan_speed),
        "cpu_temp": ("cpu_temp", DOUBLE, cpu_temp),
        "bytes_sent": ("bytes_sent", UINT64, net.bytes_sent),
        "bytes_recv": ("bytes_recv", UINT64, net.bytes_recv),
        "timestamp": ("timestamp", STRING, datetime.now(timezone.utc).isoformat())
    }

    if battery is not None:
        metrics["battery_percent"] = ("battery_percent", DOUBLE, battery.percent)

    # Deltas
    if previous_metrics:
        delta = {k: v for k, v in metrics.items()
                 if previous_metrics.get(k, (None, None, None))[2] != v[2]}
        previous_metrics = metrics
        return delta
    else:
        previous_metrics = metrics
        return metrics

# -------------------- 🔌 MQTT CALLBACKS --------------------
def on_connect(client, userdata, flags, rc):
    global connected, first, previous_metrics
    first = True
    previous_metrics = {}

    if rc == 0:
        print("[MQTT] Connected")
        connected = True
        client.subscribe(TOPIC_DCMD)

        # NBIRTH
        node_metrics = read_metrics()
        node_metrics.update({
            "online": ("online", BOOLEAN, True),
            "node": ("node", STRING, NODE_ID)
        })
        nbirth_payload = build_payload(node_metrics, is_birth=True)
        client.publish(TOPIC_NBIRTH, nbirth_payload, qos=0)
        print("[MQTT] NBIRTH published")

        # DBIRTH
        device_metrics = read_metrics()
        device_metrics.update({
            "online": ("online", BOOLEAN, True),
            "device": ("device", STRING, DEVICE_ID)
        })
        dbirth_payload = build_payload(device_metrics, is_birth=True)
        client.publish(TOPIC_DBIRTH, dbirth_payload, qos=0)
        print("[MQTT] DBIRTH published")

        time.sleep(5)  # Kritisk pause for å sikre rekkefølge
    else:
        print(f"[MQTT] Connection failed: {rc}")

def on_publish(client, userdata, mid):
    print(f"[MQTT] Published mid={mid}")

def on_dcmd(client, userdata, msg):
    payload = spb.Payload()
    payload.ParseFromString(msg.payload)
    for metric in payload.metrics:
        if metric.name == "boolean_command":
            current = os.path.isfile(COMMAND_FILE) and open(COMMAND_FILE).read().strip().upper() == "TRUE"
            new_value = not current
            with open(COMMAND_FILE, "w") as f:
                f.write("TRUE" if new_value else "FALSE")
            print(f"[DCMD] boolean_command toggled: {current} → {new_value}")

# -------------------- 🔧 MQTT SETUP --------------------
client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
client.tls_insecure_set(False)
client.on_connect = on_connect
client.on_publish = on_publish
client.message_callback_add(TOPIC_DCMD, on_dcmd)

client.connect(BROKER, PORT, 60)
client.loop_start()

while not connected:
    time.sleep(0.1)

# -------------------- 🔁 MAIN LOOP --------------------
while True:
    metrics = read_metrics()
    if not metrics:
        print("[SKIPPET] Ingen endringer siden forrige gang.")
        time.sleep(60)
        continue

    # Fjern meta-felt før DDATA
    metrics = {k: v for k, v in metrics.items() if k not in ("online", "node", "device")}

    if first:
        print("[DDATA] Første runde, sender full state")
        metrics = previous_metrics  # Full payload første gang
        first = False

    payload = build_payload(metrics)
    client.publish(TOPIC_DDATA, payload, qos=0)
    print(f"[Sent DDATA] {{ {', '.join(f'{k}: {v[2]}' for k, v in metrics.items())} }}")

    time.sleep(60)
