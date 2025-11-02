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
BROKER = "s2.eu.hivemq.cloud"
PORT = 8883  # Secure port with TLS
USERNAME = "cncoze"
PASSWORD = "passwd"
GROUP_ID = "cncoze"  # Sparkplug group ID for logical grouping of devices
NODE_ID = socket.gethostname()  # Use the computer's hostname as node ID

# Define the Sparkplug topics for birth, data, and command messages
DEVICE_ID = "system_monitor"  # eller en annen enhets-ID du velger
# NBIRTH og NDATA har kun 4 deler i topic!
TOPIC_NBIRTH = f"spBv1.0/{GROUP_ID}/NBIRTH/{NODE_ID}"  # Node birth topic
TOPIC_NDATA = f"spBv1.0/{GROUP_ID}/NDATA/{NODE_ID}"  # Regular data messages
# DCMD og DDATA har 5 deler (inkl. device_id)
TOPIC_DCMD = f"spBv1.0/{GROUP_ID}/DCMD/{NODE_ID}/{DEVICE_ID}"  # Incoming command topic from SCADA
TOPIC_DDATA = f"spBv1.0/{GROUP_ID}/DDATA/{NODE_ID}/{DEVICE_ID}" # Outgoing command data topic to SCADA
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

# Globals
connected = False  # Keeps track of whether the MQTT connection has been established
seq = 0  # Sequence number used in every Sparkplug B message, resets after 255
first = True
previous_metrics = {}

# Each metric has a unique alias number.
# This helps the receiving system (like Ignition SCADA) know what each value means
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
    "battery_percent": 12  # bare hvis batteri finnes
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
    global previous_metrics
    """
    Read system metrics like CPU, memory, disk, battery, and sensors.
    Returns a dictionary of values to be sent in the Sparkplug payload.
    """
    temps = psutil.sensors_temperatures()
    fans = psutil.sensors_fans()
    battery = psutil.sensors_battery()
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()

    # Get fan speed (if available)
    fan_speed = 0
    for device in fans.values():
        if device and isinstance(device, list):
            fan_speed = device[0].current
            break

#    # Get CPU temperature
#    cpu_temp = 0.0
#    for sensor in temps.get("coretemp", []):
#        if sensor.label == "Package id 0":
#            cpu_temp = sensor.current
#            break
    
    # Highest CPU temperature from all sources
    cpu_temp = 0.0
    for name, entries in temps.items():
        for entry in entries:
            if entry.current > cpu_temp:
                cpu_temp = entry.current

#    return {
    # Build metrics dictionary
    metrics = {
        "cpu_percent": ("cpu_percent", DOUBLE, psutil.cpu_percent()),
        "mem_used_mb": ("mem_used_mb", INT64, mem.used // (1024 * 1024)),
        "mem_total_mb": ("mem_total_mb", INT64, mem.total // (1024 * 1024)),
        "disk_used_gb": ("disk_used_gb", INT64, disk.used // (1024 ** 3)),
        "disk_total_gb": ("disk_total_gb", INT64, disk.total // (1024 ** 3)),
#        "battery_percent": ("battery_percent", DOUBLE, battery.percent if battery else -1),
        "fan_speed": ("fan_speed", INT64, fan_speed),
        "cpu_temp": ("cpu_temp", DOUBLE, cpu_temp),
        "bytes_sent": ("bytes_sent", UINT64, net.bytes_sent),
        "bytes_recv": ("bytes_recv", UINT64, net.bytes_recv),
        "timestamp": ("timestamp", STRING, datetime.now(timezone.utc).isoformat())
    }

    # Bare inkluder battery_percent hvis det finnes et batteri
    if battery is not None:
        metrics["battery_percent"] = ("battery_percent", DOUBLE, battery.percent)

    # Optimalisering: send kun endringer (delta) hvis previous_metrics er satt
    if previous_metrics:
        delta_metrics = {}
        for key, value in metrics.items():
            prev_value = previous_metrics.get(key)
            if prev_value is None or prev_value[2] != value[2]:
                delta_metrics[key] = value
        previous_metrics = metrics
        return delta_metrics
    else:
        previous_metrics = metrics
        return metrics
    
def on_connect(client, userdata, flags, rc):
    global connected, first, previous_metrics
    first = True
    previous_metrics = {}  # Tving full opplisting etter reconnect
    """
    Called when the client connects to the MQTT broker.
    Sends a NBIRTH message with initial system state.
    """
    if rc == 0:
        print("[MQTT] Connected")
        connected = True
        client.subscribe(TOPIC_DCMD)

        # --- Node Birth ---
        node_metrics = read_metrics()
        node_metrics.update({
            "online": ("online", BOOLEAN, True),
            "node": ("node", STRING, NODE_ID)
        })
        nbirth_payload = build_payload(node_metrics, is_birth=True)
        client.publish(TOPIC_NBIRTH, nbirth_payload, qos=0)
        print("[MQTT] NBIRTH published")

        # --- Device Birth ---
        device_metrics = read_metrics()
        device_metrics.update({
            "online": ("online", BOOLEAN, True),
            "device": ("device", STRING, DEVICE_ID)
        })
        dbirth_payload = build_payload(device_metrics, is_birth=True)
        TOPIC_DBIRTH = f"spBv1.0/{GROUP_ID}/DBIRTH/{NODE_ID}/{DEVICE_ID}"
        client.publish(TOPIC_DBIRTH, dbirth_payload, qos=0)
        print("[MQTT] DBIRTH published")
        time.sleep(5)  # Vent på at Ignition oppretter taggene

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

# Main loop to read metrics and send Sparkplug B data messages
while True:
    metrics = read_metrics()

    if not metrics:
        print("[SKIPPET] Ingen endringer siden forrige sending.")
        time.sleep(60)
        continue

    if first:
        topic = TOPIC_DDATA  # Samme topic
        metrics = previous_metrics  # Send full state første gang
        first = False
    else:
        topic = TOPIC_DDATA

    payload = build_payload(metrics)
    client.publish(topic, payload, qos=0)

    print(f"[Sent Sparkplug B {'NDATA' if topic == TOPIC_NDATA else 'DDATA'}]",
          {k: v[2] for k, v in metrics.items()})
    time.sleep(60)
