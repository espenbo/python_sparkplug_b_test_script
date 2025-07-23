# send_thinkfan_sparkplug

Publishes system data (temperature, CPU, RAM, disk, fan status) as Sparkplug B messages to HiveMQ Cloud via MQTT.

## Features

- Sends an NBIRTH message upon connection with all metrics (including online and node)
- Sends an NDATA message every 60 seconds
- Supports DCMD (`boolean_command`) for toggling actions

## Prerequisites

You need:

    The original sparkplug_b.proto file.

    protoc installed.

    The protobuf Python package installed (pip install protobuf — already in your requirements.txt).

    Step 1
    wget https://raw.githubusercontent.com/eclipse-tahu/tahu/refs/heads/master/sparkplug_b/sparkplug_b.proto

    Step 2: Install protoc Compiler (if needed)
    sudo apt install -y protobuf-compiler

    Step 3: Compile the .proto File. Run the following command from the same directory:
    protoc --proto_path=. --python_out=. sparkplug_b.proto

    This generates:
    sparkplug_b_pb2.py

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python send_thinkfan_sparkplug.py
```

## MQTT-oppsett

- Broker: `broker.hivemq.com`
- Port: `8883` (TLS)
- Username/Password: Set directly in the script
- Format: Sparkplug B v1.0

## MQTT Topics

- NBIRTH: `spBv1.0/thinkfan/NBIRTH/<hostname>`
- NDATA: `spBv1.0/thinkfan/NDATA/<hostname>`
- DCMD: `spBv1.0/thinkfan/DCMD/<hostname>`

