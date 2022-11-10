#!/usr/bin/bash
wget https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.2.4/hadoop-3.2.4.tar.gz

docker build . \
    --build-arg "HTTP_PROXY=http://127.0.0.1:7890" \
    --build-arg "HTTPS_PROXY=http://127.0.0.1:7890/" \
    --build-arg "NO_PROXY=localhost,127.0.0.1" \
    --network host -f base.Dockerfile \
    -t hdfs-base

docker build . -f namenode.Dockerfile -t hdfs-namenode