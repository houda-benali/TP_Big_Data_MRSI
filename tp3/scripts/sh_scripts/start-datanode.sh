#!/bin/bash
set -e

mkdir -p /data/data

# Démarrer le NodeManager en arrière-plan
yarn nodemanager &
YARN_PID=$!

# Démarrer le DataNode
hdfs datanode &
HDFS_PID=$!

# Surveiller uniquement le DataNode
set +e
wait "$HDFS_PID"
STATUS=$?

# Si le DataNode s'arrête, arrêter le NodeManager
kill "$YARN_PID" 2>/dev/null
wait "$YARN_PID" 2>/dev/null

exit "$STATUS"
