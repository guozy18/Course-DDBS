# Course-DDBS

## Docker Config
Docker 目前完成了对 mysql 服务器以及对 HDFS 服务的基本配置，通过 docker compose 组合使用，现在的配置如下：

- 启动1个hdfs namenode，名称(hostname)为namenode
- 启动2个hdfs datanode，名称(hostname)为datanode1, datanode2
- 启动2个mysql server，名称(hostname)为mysql1, mysql2，用户均为root，对应密码为mysql1, mysql2
- 所有的容器都绑定到一个network（类型为bridge），名称为docker-config_ddbms-network

以上信息可以从`docker-config/docker-compose.yml`中查看到

### 环境准备
需要手动完成的目前只有 *准备课程提供的数据文件夹*

```shell
# 移动课程文件夹到当前目录，注意目前必须移动到./sql-data下
mv /path/to/your/db-generation ./sql-data
```

为了正确运行多个docker环境，需要有一些准备工作：
1. 需要提前 build 使用的 docker image
2. 需要提前生成好两个DBMS需要的数据（以`.sql`的格式）

```shell
# 如果使用实验室服务器，它和外网不通，为了正确拉取镜像可能需要配置代理，可以在init.sh中修改参数PROXY_ADDR
# 另外如果使用实验室服务器，可以用以下方式验证 docker image 已经存在了，那么不需要build docker image了
docker image ls
# 如果能够看到hdfs-base, server, hdfs-namenode，mysql 8.0，那么无需再build docker image
cd scripts && ./init.sh --no-docker && cd ..
# 否则还是需要执行 docker build
cd scripts && ./init.sh && cd ..
# 此时可以验证 ./sql-data/ 下已经有了对应shard的.sql
# 同时docker image ls 也应该能看到hdfs-base, server这样的镜像
```
### docker 常用命令
```shell
# launch all containers
cd docker-config
docker compose -p hjl up

# stop all containers
docker compose -p hjl stop

# remove all containers
docker compose -p hjl down

# restart all containers
docker compose -p hjl restart

# connect mysql server with mysql client
docker run -it --network hjl_ddbms-network --rm mysql:8.0 mysql -hmysql1 -uroot -p
docker run -it --network hjl_ddbms-network --rm mysql:8.0 mysql -hmysql2 -uroot -p

# show the logs of dbserver
docker compose -p hjl logs server1

# run the client-test or other binary
docker run -it --rm --network hjl_ddbms-network -v/path/to/Course-DDBS:/root/Course-DDBS:ro server /bin/bash

# show all docker networks
docker network ls

# show all docker images
docker image ls
```
### 各个服务部件的监听地址要求
- mysql服务是常规的监听在3306端口，具体监听的ip地址不太清楚，我估计是0.0.0.0:3306
- dbserver服务需要显式给出监听的地址，在 docker compose 中直接使用server1/2:27023
    - 它需要在创建的时候就向control发送自己的地址，为了简单并没有在运行时检查自己的ip地址，而是在启动的时候用命令行传入监听的地址
- control服务因为要接受来自客户端的请求，因此肯定不能只绑定到docker内的网络中，它默认的地址是0.0.0.0:27022
### docker compose 端口对应关系说明
首先 mysql, server, control 它们目前都没有把端口publish给host，因此只能内部测试使用，后期应该只需要把control暴露给 host 方便客户端使用

- prometheus：这是一个用于检测集群状态的开源工具，它暴露到host的端口为9090
- grafana: 这是一个基于web的可视化面板，它能够把prometheus的数据呈现给用户，暴露到host的端口为9100