#!/usr/bin/bash
SQL_DATA_DIR="../sql-data"
DATA_GEN_SCRIPTS="genTable_sql_relationalDB10G.py"

function build_docker_image() {
    local scripts_dir=$(dirname $(readlink -f $0))
    cd ${scripts_dir}/../docker-config

    if [ ! -e hadoop-3.2.4.tar.gz ];then
        echo "Download hadoop tar..."
        wget https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.2.4/hadoop-3.2.4.tar.gz
    fi

    docker build . \
        --build-arg "HTTP_PROXY=http://127.0.0.1:7890" \
        --build-arg "HTTPS_PROXY=http://127.0.0.1:7890/" \
        --build-arg "NO_PROXY=localhost,127.0.0.1" \
        --network host -f base.Dockerfile \
        -t hdfs-base

    docker build . \
        --build-arg "HTTP_PROXY=http://127.0.0.1:7890" \
        --build-arg "HTTPS_PROXY=http://127.0.0.1:7890/" \
        --build-arg "NO_PROXY=localhost,127.0.0.1" \
        --network host -f server.Dockerfile \
        -t server

    docker build . -f namenode.Dockerfile -t hdfs-namenode

    # do not init hdfs for noe
    # docker build . -f init-hdfs.Dockerfile -t init-hdfs
    cd $original_dir
}

function generate_sql_data() {
    local scripts_dir=$(dirname $(readlink -f $0))
    cd $scripts_dir && cd $SQL_DATA_DIR
    if [ ! -e user.sql ] || [ ! -e user_read.sql ] || [ ! -e article.sql ] || [ ! -d articles ]; then
        if [ ! -e $DATA_GEN_SCRIPTS ]; then
            echo "There is no $DATA_GEN_SCRIPTS in $(pwd)"
            exit 1
        fi
        python3 $DATA_GEN_SCRIPTS
    fi
    cd $scripts_dir
    python3 make_shard.py -s $SQL_DATA_DIR
}

original_dir=$(dirname $(readlink -f $0))
echo "start build docker image"
build_docker_image
cd $original_dir
echo "start generate sql data in $SQL_DATA_DIR"
generate_sql_data
cd $original_dir
