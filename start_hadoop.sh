#!/bin/bash

# Start HDFS services (NameNode + DataNode1 + SecondaryNameNode)
start-dfs.sh

# Start DataNode2 (using alternate configuration + unique PID identity)
HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop-dn2" \
HADOOP_IDENT_STRING=dn2 \
"$HADOOP_HOME/bin/hdfs" --daemon start datanode

# Start YARN (optional)
start-yarn.sh

echo "All Hadoop services started."
