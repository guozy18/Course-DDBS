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
- grafana: 这是一个基于web的可视化面板，它能够把prometheus的数据呈现给用户，暴露到host的端口为9200

## Bulk Load

bulk load the three basic three tables
```shell
# run the client-test or other binary
docker run -it --rm --network hjl_ddbms-network -v/path/to/Course-DDBS:/root/Course-DDBS:ro server /bin/bash

# run the CLI tool
./Course-DDBS/target/debug/ddbs-client -c control:27022 -s server1:27023 -s server2:27023

# load the thee basic tables.
[Course-DDBS]> :cluster-init
```

compute and bulk load be_read and popular_rank tables
```shell
# generator and load the be read table.
[Course-DDBS]> :load-be-read

# generator and load the monthly popular table.
[Course-DDBS]> :load-monthly-popular-table
# generator and load the weekly popular table.
[Course-DDBS]> :load-weekly-popular-table
# generator and load the daily popular table.
[Course-DDBS]> :load-daily-popular-table
```


## CLI 使用手册

### 客户端启动
启动docker来运行客户端，从而运行CLI，能够从CLI输入内置命令来初始化集群，动态生成be_read和popular_rank数据；也能够直接输入SQL语句来执行。
```shell
# run the client-test or other binary
docker run -it --rm --network hjl_ddbms-network -v/path/to/Course-DDBS:/root/Course-DDBS:ro server /bin/bash

# run the CLI tool
./Course-DDBS/target/debug/ddbs-client -c control:27022 -s server1:27023 -s server2:27023
```

### CLI内置命令
包括集群初始化，动态生成be_read和popular_rank等表格
```shell
# all the inner-command list
[Course-DDBS]> :h
:q - Quit this application.
:h - Show help message.
:cluster-init - Init Database cluster.
:load-be-read - generator and load the be read table.
:load-monthly-popular-table - generator and load the monthly popular table.
:load-weekly-popular-table - generator and load the weekly popular table.
:load-daily-popular-table - generator and load the daily popular table.

# Init Database cluster.
[Course-DDBS]> :cluster-init

# generator and load the be read table.
[Course-DDBS]> :load-be-read
```

### CLI输入SQL语句并直接执行

```shell
# insert new  user
[Course-DDBS]> INSERT INTO user VALUES ('1000000006','utest','test','test','male','emailtest','phonetest','depttest',
               'grade1','zh','Beijing','role1','tages10','15');

# update user
[Course-DDBS]> UPDATE user SET name='test1',gender='female' WHERE id='utest';

# common query
[Course-DDBS]> SELECT * from user limit 5;
[Course-DDBS]> SELECT  * from user WHERE region='Beijing' limit 5;
[Course-DDBS]> SELECT  * from user WHERE region='Hong Kong' limit 5;
[Course-DDBS]> SELECT  * from user WHERE region='Beijing' AND region='Hong Kong' limit 5;

# complex query - inner join
[Course-DDBS]> SELECT a.name,region, a.uid, b.uid FROM user AS a INNER JOIN user_read AS b ON a.uid = b.uid where a.region = "Beijing" LIMIT 5; 
[Course-DDBS]> SELECT a.name,region, a.uid, b.uid, b.aid FROM user AS a INNER JOIN user_read AS b ON a.uid = b.uid where a.region = "Beijing" AND a.uid='5555' LIMIT 5;
```

