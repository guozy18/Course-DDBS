# Course-DDBS

## Docker Config
Docker 目前完成了对 mysql 服务器以及对 HDFS 服务的基本配置，通过 docker compose 组合使用，现在的配置如下：

- 启动1个hdfs namenode，名称(hostname)为namenode
- 启动2个hdfs datanode，名称(hostname)为datanode1, datanode2
- 启动2个mysql server，名称(hostname)为mysql1, mysql2，用户均为root，对应密码为mysql1, mysql2
- 所有的容器都绑定到一个network（类型为bridge），名称为docker-config_ddbms-network

以上信息可以从`docker-config/docker-compose.yml`中查看到

### build与运行
```shell
# build hdfs image
cd docker-config/hdfs
./build.sh

# launch all containers
cd ..
docker compose up
```

### 验证mysql server正常工作
```shell
docker run -it --network docker-config_ddbms-network --rm mysql:8.0 mysql -hmysql1 -uroot -p
docker run -it --network docker-config_ddbms-network --rm mysql:8.0 mysql -hmysql2 -uroot -p
```