FROM hdfs-base

WORKDIR /root
RUN hdfs namenode -format
CMD hdfs namenode