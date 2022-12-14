original_dir=$(dirname $(readlink -f $0))
echo $original_dir

node_expoter_bin="/mnt/nvme1/dreamHuang/node_exporter-1.5.0/node_exporter"
pkill node_exporter
cd $original_dir/../docker-config
$node_expoter_bin --collector.systemd --collector.processes 2>&1 > node_exporter.log &
docker compose -p hjl down && cargo build && docker compose -p hjl up -d
