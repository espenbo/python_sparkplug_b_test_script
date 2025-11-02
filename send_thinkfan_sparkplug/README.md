# send_thinkfan_sparkplug

Publishes live system metrics (CPU, memory, temperature, disk, fan, network) as **Sparkplug B** messages over **MQTT**. Designed for **HiveMQ Cloud** and **Ignition MQTT Engine**.


## Features

- Sends **NBIRTH** on connection (node-level metadata)
- Sends **DBIRTH** with full metric set (for alias/tag creation)
- Sends **DDATA** updates every 60 seconds (deltas only)
- Handles **DCMD** (`boolean_command`) from SCADA
- Fully compliant with **Sparkplug B v1.0**

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

## Environment Setup (.env)

Create a .env file in the root directory:
BROKER=your-mqtt-broker-url
PORT=8883
USERNAME=your-hivemq-username
PASSWORD=your-hivemq-password
GROUP_ID=your_sparkplug_group
DEVICE_ID=system_monitor
COMMAND_FILE=command_flag.txt

Run on Startup (systemd)

You can run the script automatically at boot using systemd.

1. Create a service file
# /etc/systemd/system/sparkplug_monitor.service
[Unit]
Description=Sparkplug System Monitor
After=network.target

[Service]
ExecStart=/home/username/path/to/.venv/bin/python /home/username/path/to/send_thinkfan_sparkplug_V5.py
WorkingDirectory=/home/username/path/to/
User=username
EnvironmentFile=/home/username/path/to/.env
Restart=always

[Install]
WantedBy=multi-user.target

2. Enable and start the service
sudo systemctl daemon-reload
sudo systemctl enable sparkplug_monitor.service
sudo systemctl start sparkplug_monitor.service

To check the status:
sudo systemctl status sparkplug_monitor.service