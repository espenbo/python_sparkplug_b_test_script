#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sparkplug B Edge Node - System Monitor (V5)
-------------------------------------------

Publishes NBIRTH, DBIRTH, DDATA and handles DCMD messages to HiveMQ Cloud,
intended for use with Ignition MQTT Engine.

Publishes:
  • NBIRTH - node birth message (metadata)
  • DBIRTH - device birth message (initial state)
  • DDATA  - periodic system metrics (deltas)
Also handles incoming DCMD commands from SCADA/MQTT Engine.
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
from dotenv import load_dotenv # Import configuration settings


load_dotenv()  # Must be called before os.getenv()

# Configuration for connecting to the MQTT broker (HiveMQ Cloud)
# https://www.mqtt-dashboard.com/
# Load environment variables from .env file
BROKER       = os.getenv("BROKER")
PORT         = int(os.getenv("PORT", 8883))  # fallback Secure port with TLS
USERNAME     = os.getenv("USERNAME")
PASSWORD     = os.getenv("PASSWORD")
GROUP_ID     = os.getenv("GROUP_ID")
DEVICE_ID    = os.getenv("DEVICE_ID")
NODE_ID      = socket.gethostname()

COMMAND_FILE = os.getenv("COMMAND_FILE", "command_flag.txt")

# Remove command file if it exists (reset DCMD toggle)
if os.path.isfile(COMMAND_FILE):
    os.remove(COMMAND_FILE)

# ─────────────────────────────────────────────────────────────────────────────
# Sparkplug B‑topics
# ─────────────────────────────────────────────────────────────────────────────
TOPIC_NBIRTH = f"spBv1.0/{GROUP_ID}/NBIRTH/{NODE_ID}"
TOPIC_NDATA  = f"spBv1.0/{GROUP_ID}/NDATA/{NODE_ID}"
TOPIC_DBIRTH = f"spBv1.0/{GROUP_ID}/DBIRTH/{NODE_ID}/{DEVICE_ID}"
TOPIC_DDATA  = f"spBv1.0/{GROUP_ID}/DDATA/{NODE_ID}/{DEVICE_ID}"
TOPIC_DCMD   = f"spBv1.0/{GROUP_ID}/DCMD/{NODE_ID}/{DEVICE_ID}"

# ─────────────────────────────────────────────────────────────────────────────
# Sparkplug B data types (matching sparkplug_b.proto)
# ─────────────────────────────────────────────────────────────────────────────
INT8, INT16, INT32, INT64 = 1, 2, 3, 4
UINT8, UINT16, UINT32, UINT64 = 5, 6, 7, 8
FLOAT, DOUBLE, BOOLEAN, STRING, DATETIME = 9, 10, 11, 12, 13

# ─────────────────────────────────────────────────────────────────────────────
# Global variables and tag aliases
# ─────────────────────────────────────────────────────────────────────────────
connected = False
seq = 0
first = True
previous_metrics = {}

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
    "battery_percent": 12,
    "device": 13
}

# ─────────────────────────────────────────────────────────────────────────────
# Build Sparkplug B payload
# ─────────────────────────────────────────────────────────────────────────────
def build_payload(metrics: dict, is_birth=False) -> bytes:
    """Build a Sparkplug B-compatible binary payload."""
    global seq
    payload = spb.Payload()
    payload.timestamp = int(time.time() * 1000)
    payload.seq = seq
    seq = (seq + 1) % 256

    for _, (name, datatype, value) in metrics.items():
        m = payload.metrics.add()
        m.alias = alias_map.get(name, 255)
        m.name = name
        m.timestamp = payload.timestamp
        m.datatype = datatype
        if datatype in (INT8, INT16, INT32, UINT8, UINT16, UINT32):
            m.int_value = int(value)
        elif datatype in (INT64, UINT64):
            m.long_value = int(value)
        elif datatype == FLOAT:
            m.float_value = float(value)
        elif datatype == DOUBLE:
            m.double_value = float(value)
        elif datatype == STRING:
            m.string_value = str(value)
        elif datatype == BOOLEAN:
            m.boolean_value = bool(value)

    if is_birth:
        payload.uuid = str(uuid.uuid4())
    return payload.SerializeToString()

# ─────────────────────────────────────────────────────────────────────────────
# Read system metrics using psutil
# ─────────────────────────────────────────────────────────────────────────────
def read_metrics() -> dict:
    """Leser systemdata og returnerer Sparkplug‑kompatible metrics."""
    global previous_metrics

    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net  = psutil.net_io_counters()
    temps = psutil.sensors_temperatures()
    fans  = psutil.sensors_fans()
    battery = psutil.sensors_battery()

    cpu_temp = max((e.current for v in temps.values() for e in v), default=0.0)
    fan_speed = next((f[0].current for f in fans.values() if f), 0)

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
    if battery:
        metrics["battery_percent"] = ("battery_percent", DOUBLE, battery.percent)

    if not previous_metrics:
        previous_metrics = metrics
        return metrics
    delta = {k: v for k, v in metrics.items()
             if previous_metrics.get(k, (None, None, None))[2] != v[2]}
    previous_metrics = metrics
    return delta

# ─────────────────────────────────────────────────────────────────────────────
# 🌐 MQTT callbacks
# ─────────────────────────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    """Publish NBIRTH and DBIRTH when connected to the broker."""
    global connected, first, previous_metrics, seq
    first = True
    previous_metrics = {}
    if rc == 0:
        connected = True
        print("[MQTT] Connected to broker")
        client.subscribe(TOPIC_DCMD)

        # NBIRTH
        node_metrics = {
            "online": ("online", BOOLEAN, True),
            "node": ("node", STRING, NODE_ID)
        }
        nbirth_payload = build_payload(node_metrics, is_birth=True)
        client.publish(TOPIC_NBIRTH, nbirth_payload, qos=0)
        print("[MQTT] NBIRTH published")

        # DBIRTH: Full device metric list (creates tags in Ignition)
        device_metrics = {
            "cpu_percent": ("cpu_percent", DOUBLE, 0.0),
            "mem_used_mb": ("mem_used_mb", INT64, 0),
            "mem_total_mb": ("mem_total_mb", INT64, 0),
            "disk_used_gb": ("disk_used_gb", INT64, 0),
            "disk_total_gb": ("disk_total_gb", INT64, 0),
            "fan_speed": ("fan_speed", INT64, 0),
            "cpu_temp": ("cpu_temp", DOUBLE, 0.0),
            "bytes_sent": ("bytes_sent", UINT64, 0),
            "bytes_recv": ("bytes_recv", UINT64, 0),
            "timestamp": ("timestamp", STRING, ""),
            "battery_percent": ("battery_percent", DOUBLE, 0.0),
            "online": ("online", BOOLEAN, True),
            "device": ("device", STRING, DEVICE_ID),
        }
        dbirth_payload = build_payload(device_metrics, is_birth=True)
        client.publish(TOPIC_DBIRTH, dbirth_payload, qos=0)
        print("[MQTT] DBIRTH published")

        time.sleep(5)  # Allow Ignition time to process tag creation
    else:
        print(f"[MQTT] Connection failed: {rc}")

def on_publish(client, userdata, mid):
    print(f"[MQTT] Published message ID {mid}")

def on_dcmd(client, userdata, msg):
    """Example handler for incoming DCMD (boolean_command)."""
    payload = spb.Payload()
    payload.ParseFromString(msg.payload)
    for metric in payload.metrics:
        if metric.name == "boolean_command":
            current = os.path.isfile(COMMAND_FILE) and open(COMMAND_FILE).read().strip().upper() == "TRUE"
            new = not current
            with open(COMMAND_FILE, "w") as f:
                f.write("TRUE" if new else "FALSE")
            print(f"[DCMD] boolean_command toggled {current} → {new}")

# ─────────────────────────────────────────────────────────────────────────────
# MQTT Client Setup
# ─────────────────────────────────────────────────────────────────────────────
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.username_pw_set(USERNAME, PASSWORD)
client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
client.on_connect = on_connect
client.on_publish = on_publish
client.message_callback_add(TOPIC_DCMD, on_dcmd)

client.connect(BROKER, PORT, 60)
client.loop_start()

# Wait until connected before continuing
while not connected:
    time.sleep(0.2)

# ─────────────────────────────────────────────────────────────────────────────
# Main loop – Send DDATA every 60 seconds
# ─────────────────────────────────────────────────────────────────────────────
while True:
    metrics = read_metrics()
    if not metrics:
        print("[SKIPPED] No changes.")
        time.sleep(60)
        continue

    # Send all metrics on first DDATA publish
    if first:
        print("[DDATA] Sending full set (first time).")
        metrics = previous_metrics
        first = False

    payload = build_payload(metrics)
    client.publish(TOPIC_DDATA, payload, qos=0)
    print("[DDATA] Sendt:", ", ".join(f"{k}: {v[2]}" for k, v in metrics.items()))
    time.sleep(60)
