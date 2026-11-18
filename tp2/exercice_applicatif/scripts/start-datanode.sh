#!/bin/bash

set -e

echo "Starting DataNode..."
echo "Hostname: $(hostname)"

exec hdfs datanode

