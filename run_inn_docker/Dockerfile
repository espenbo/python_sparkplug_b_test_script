FROM python:3.11-slim

# Install system packages for psutil and the protobuf compiler
RUN apt-get update && \
    apt-get install -y gcc libffi-dev libssl-dev python3-dev \
                       build-essential protobuf-compiler && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


# Set working directory
WORKDIR /app

# Copy code and requirements file
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project
COPY . .

# Compile the .proto file
RUN protoc --proto_path=. --python_out=. sparkplug_b.proto

# Default command
CMD ["python", "send_sparkplug.py"]
