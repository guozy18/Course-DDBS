FROM hdfs-base
WORKDIR /root

COPY ./init-hdfs-entrypoint.sh .
ENTRYPOINT ["/root/init-hdfs-entrypoint.sh"]