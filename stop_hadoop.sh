#!/bin/bash

# Stop DataNode2 (uses alternate config + custom ident)
HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop-dn2" \
HADOOP_IDENT_STRING=dn2 \
"$HADOOP_HOME/bin/hdfs" --daemon stop datanode

# Stop YARN services
stop-yarn.sh

# Stop HDFS services (NameNode + DataNode1 + SecondaryNameNode)
stop-dfs.sh

echo "Hadoop services stopped."
