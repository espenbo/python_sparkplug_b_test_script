# Import standard and external Python libraries
import os  # Used for file operations like checking if a file exists
import time  # Used for waiting and time calculations
import socket  # To get the system's hostname
import ssl  # For secure communication using TLS/SSL
import uuid  # To generate unique identifiers for devices
import psutil  # To access system-level metrics like CPU, memory, etc.
import paho.mqtt.client as mqtt  # MQTT client for connecting to HiveMQ Cloud
from datetime import datetime  # To get current timestamps in a readable format
from sparkplug_b import sparkplug_b_pb2 as spb  # Sparkplug B protocol buffers for message encoding

# Configuration for connecting to the MQTT broker (HiveMQ Cloud)
# https://www.mqtt-dashboard.com/
BROKER = "broker.hivemq.cloud"
PORT = 8883  # Secure port with TLS
USERNAME = "lenovo_p53"
PASSWORD = "PASSWORD"
GROUP_ID = "Lenovo_Black"  # Sparkplug group ID for logical grouping of devices
NODE_ID = socket.gethostname()  # Use the computer's hostname as node ID

# Define the Sparkplug topics for birth, data, and command messages
TOPIC_NBIRTH = f"spBv1.0/{GROUP_ID}/NBIRTH/{NODE_ID}"  # Node birth topic
TOPIC_NDATA = f"spBv1.0/{GROUP_ID}/NDATA/{NODE_ID}"  # Regular data messages
TOPIC_DCMD = f"spBv1.0/{GROUP_ID}/DCMD/{NODE_ID}"  # Incoming command topic from SCADA

COMMAND_FILE = "command_flag.txt"  # File used to store command state (for toggling example)

# Sparkplug B metric data types - these must match the Sparkplug B specification
INT8 = 1
INT16 = 2
INT32 = 3
INT64 = 4
UINT8 = 5
UINT16 = 6
UINT32 = 7
UINT64 = 8
FLOAT = 9
DOUBLE = 10
BOOLEAN = 11
STRING = 12
DATETIME = 13

connected = False  # Keeps track of whether the MQTT connection has been established
seq = 0  # Sequence number used in every Sparkplug B message, resets after 255

# Each metric has a unique alias number.
# This helps the receiving system (like Ignition SCADA) know what each value means
alias_map = {
    "cpu_percent": 0,
    "mem_used_mb": 1,
    "mem_total_mb": 2,
    "disk_used_gb": 3,
    "disk_total_gb": 4,
    "battery_percent": 5,
    "fan_speed": 6,
    "cpu_temp": 7,
    "timestamp": 8,
    "online": 9,
    "node": 10
}

def build_payload(metrics: dict, is_birth=False) -> bytes:
    """
    Create a Sparkplug B payload message from the metrics.
    This function encodes system metrics into a binary format that can be sent over MQTT.
    """
    global seq
    payload = spb.Payload()
    payload.timestamp = int(time.time() * 1000)  # Current time in ms
    payload.seq = seq  # Set sequence number
    seq = (seq + 1) % 256  # Increment and wrap sequence number

    for key, (name, datatype, value) in metrics.items():
        metric = payload.metrics.add()
        metric.alias = alias_map.get(name, 255)  # Use alias from map
        metric.name = name
        metric.timestamp = payload.timestamp
        metric.datatype = datatype

        # Set the correct value field based on the datatype
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

    # Add a unique identifier only if this is a birth certificate (NBIRTH)
    if is_birth:
        payload.uuid = str(uuid.uuid4())

    return payload.SerializeToString()  # Convert the payload to binary for sending

def read_metrics():
    """
    Read system metrics like CPU, memory, disk, battery, and sensors.
    Returns a dictionary of values to be sent in the Sparkplug payload.
    """
    temps = psutil.sensors_temperatures()
    fans = psutil.sensors_fans()
    battery = psutil.sensors_battery()
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    # Get fan speed (if available)
    fan_speed = 0
    for device in fans.values():
        if device and isinstance(device, list):
            fan_speed = device[0].current
            break

    # Get CPU temperature
    cpu_temp = 0.0
    for sensor in temps.get("coretemp", []):
        if sensor.label == "Package id 0":
            cpu_temp = sensor.current
            break

    return {
        "cpu_percent": ("cpu_percent", DOUBLE, psutil.cpu_percent()),
        "mem_used_mb": ("mem_used_mb", INT64, mem.used // (1024 * 1024)),
        "mem_total_mb": ("mem_total_mb", INT64, mem.total // (1024 * 1024)),
        "disk_used_gb": ("disk_used_gb", INT64, disk.used // (1024 ** 3)),
        "disk_total_gb": ("disk_total_gb", INT64, disk.total // (1024 ** 3)),
        "battery_percent": ("battery_percent", DOUBLE, battery.percent if battery else -1),
        "fan_speed": ("fan_speed", INT64, fan_speed),
        "cpu_temp": ("cpu_temp", DOUBLE, cpu_temp),
        "timestamp": ("timestamp", STRING, datetime.utcnow().isoformat())
    }

def on_connect(client, userdata, flags, rc):
    """
    Called when the client connects to the MQTT broker.
    Sends a NBIRTH message with initial system state.
    """
    global connected
    if rc == 0:
        print("[MQTT] Connected")
        connected = True  # Mark as connected so we can start publishing data
        client.subscribe(TOPIC_DCMD)  # Listen for incoming commands from SCADA
        metrics = read_metrics()
        metrics.update({
            "online": ("online", BOOLEAN, True),
            "node": ("node", STRING, NODE_ID)
        })
        birth_payload = build_payload(metrics, is_birth=True)
        client.publish(TOPIC_NBIRTH, birth_payload, qos=0)
        print("[MQTT] NBIRTH published")
    else:
        print(f"[MQTT] Connection failed: {rc}")

def on_publish(client, userdata, mid):
    """
    Called when a message is successfully published to the MQTT broker.
    Useful for debugging.
    """
    print(f"[MQTT] Published message ID: {mid}")

def on_dcmd(client, userdata, msg):
    """
    Handle incoming command messages (DCMD) from SCADA.
    This example looks for a 'boolean_command' tag and toggles a local file value.
    """
    payload = spb.Payload()
    payload.ParseFromString(msg.payload)
    for metric in payload.metrics:
        if metric.name == "boolean_command":
            current = False
            if os.path.isfile(COMMAND_FILE):
                with open(COMMAND_FILE, "r") as f:
                    current = f.read().strip().upper() == "TRUE"
            new_value = not current  # Toggle the state
            with open(COMMAND_FILE, "w") as f:
                f.write("TRUE" if new_value else "FALSE")
            print(f"[DCMD] boolean_command toggled: {current} → {new_value}")

# MQTT client setup and security configuration
client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
client.tls_insecure_set(False)
client.on_connect = on_connect
client.on_publish = on_publish
client.message_callback_add(TOPIC_DCMD, on_dcmd)

# Connect to the broker and start the client loop
client.connect(BROKER, PORT, 60)
client.loop_start()

# Wait until we are connected before sending data
while not connected:
    time.sleep(0.1)

# Send system metrics continuously every 60 seconds
while True:
    metrics = read_metrics()
    payload = build_payload(metrics)

        # DEBUG: print deserialisert Sparkplug Payload
#        debug_payload = spb.Payload()
#        debug_payload.ParseFromString(payload)
#        print("[DEBUG Payload]")
#        for metric in debug_payload.metrics:
#            print(f"  - {metric.name}: {metric}")  # print the object with datatype and value to console.

    client.publish(TOPIC_NDATA, payload, qos=0)
    print("[Sent Sparkplug B]", {k: v[2] for k, v in metrics.items()})
    time.sleep(60)
