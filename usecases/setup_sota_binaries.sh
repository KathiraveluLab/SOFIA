#!/usr/bin/env bash
set -e

echo "Setting up SOTA binaries (Redis and Consul) in third_party/..."

# Create third_party directory
mkdir -p third_party

# Download and unzip Consul if not present
if [ ! -f "third_party/consul" ]; then
    echo "Downloading Consul..."
    wget -q https://releases.hashicorp.com/consul/1.18.1/consul_1.18.1_linux_amd64.zip -O third_party/consul.zip
    unzip -q -o third_party/consul.zip -d third_party
    rm -f third_party/consul.zip
else
    echo "Consul already set up."
fi

# Download and compile Redis if not present
if [ ! -f "third_party/redis-7.2.4/src/redis-server" ]; then
    echo "Downloading and compiling Redis..."
    wget -q https://download.redis.io/releases/redis-7.2.4.tar.gz -O third_party/redis-7.2.4.tar.gz
    tar -xzf third_party/redis-7.2.4.tar.gz -C third_party
    rm -f third_party/redis-7.2.4.tar.gz
    cd third_party/redis-7.2.4 && make -s MALLOC=libc
    cd ../..
else
    echo "Redis already set up."
fi

echo "SOTA setup complete!"
