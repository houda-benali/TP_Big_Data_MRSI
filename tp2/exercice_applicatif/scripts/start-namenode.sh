#!/bin/bash

set -e

HOSTNAME=$(hostname)

echo "===================================="
echo "Starting Hadoop NameNode: $HOSTNAME"
echo "===================================="

mkdir -p /data/name

if [ -f /data/name/current/VERSION ]; then

    echo "NameNode metadata already exists."
    echo "Skipping initialization."

else

    if [ "$HOSTNAME" = "nn1" ]; then

        echo "First initialization of nn1..."
        echo "Formatting NameNode nn1."

        hdfs namenode -format -nonInteractive

    elif [ "$HOSTNAME" = "nn2" ]; then

        echo "First initialization of nn2..."
        echo "Waiting for nn1 RPC..."

        until timeout 3 bash -c '</dev/tcp/nn1/8020' 2>/dev/null; do
            sleep 5
        done

        echo "nn1 RPC is available."

        echo "Initializing nn2 from nn1..."

        hdfs namenode -bootstrapStandby -nonInteractive

    else

        echo "Unknown NameNode hostname: $HOSTNAME"
        exit 1

    fi

fi

echo "Starting NameNode daemon..."

exec hdfs namenode

