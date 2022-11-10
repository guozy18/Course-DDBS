FROM ubuntu:20.04

WORKDIR /root

RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends openjdk-8-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY ./hadoop-3.2.4.tar.gz /root
RUN tar xf hadoop-3.2.4.tar.gz

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=/root/hadoop-3.2.4
ENV HADOOP_HDFS_HOME=/root/hadoop-3.2.4
ENV HADOOP_CONF_DIR=/root/hadoop-3.2.4/etc/hadoop
ENV PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH

COPY ./core-site.xml $HADOOP_CONF_DIR
COPY ./hdfs-site.xml $HADOOP_CONF_DIR
COPY ./hadoop-env.sh $HADOOP_CONF_DIR

RUN mkdir -p /root/hdfs/namenode && mkdir -p /root/hdfs/datanode