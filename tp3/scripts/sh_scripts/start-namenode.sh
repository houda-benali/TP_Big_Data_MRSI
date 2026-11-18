#!/bin/bash
set -e

mkdir -p /data/name

if [ ! -f /data/name/current/VERSION ]; then
    hdfs namenode -format -force -nonInteractive
fi

yarn resourcemanager &
YARN_PID=$!

hdfs namenode &
HDFS_PID=$!

# Le NameNode reste le processus surveillé
set +e
wait "$HDFS_PID"
STATUS=$?

# Le NameNode est arrêté : arrêter le ResourceManager
kill "$YARN_PID" 2>/dev/null
wait "$YARN_PID" 2>/dev/null

exit "$STATUS"
