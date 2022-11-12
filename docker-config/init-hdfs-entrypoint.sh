#!/bin/bash

while ! hdfs dfs -ls / ; do
    echo "hdfs is not avaiable"
    sleep 5
done

if [ $# -ne 1 ]; then
    echo "please specify dir that need upload to hdfs"
    exit 1
fi

hdfs dfs -put $1 /ddbms