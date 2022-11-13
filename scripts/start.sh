# !/bin/bash
### Usage: bash ./start.sh [OPTIONS] COMMAND

start=false         # start servers or not
quit=false          # not quit servers
mode=release
dbserverport=27021
debug=false

help() {
    sed -rn 's/^### ?//;T;p;' "$0"
    exit 0
}

while [ -n "$1" ]; do
    case "$1" in
    -m | --mode)
        mode="$2"
        shift
        ;;
    -d | --debug)
        debug=true
        ;;
    -s | --start)
        start=true
        ;;
    -q | --quit)
        quit=true
        ;;
    -p | --port)
        port=$2
        shift
        ;;
    -h | --help)
        help
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Error: not defined option."
        exit 1
        ;;
    esac
    shift
done

SHELL_FOLDER=$(
    cd "$(dirname "$0")"
    pwd
)
PROJECT_PATH=$(
    cd "$SHELL_FOLDER/.."
    pwd
)
TARGET_FOLDER=$(
    cd "$PROJECT_PATH/target"
    pwd
)

SERVER_BIN_PATH=$PROJECT_PATH/target/$mode/ddbs-server
CLIENT_BIN_PATH=$PROJECT_PATH/target/$mode/client-test
export RUST_LOG=warn

echo "Shell folder is: ${SHELL_FOLDER}"
echo "Project path is: ${PROJECT_PATH}"
echo "Target folder is: ${TARGET_FOLDER}"
echo "Import config is: ${IMPORT_CONFIG}"
echo "RunServer binary path is: ${SERVER_BIN_PATH}"
echo "TestClient binary path is: ${CLIENT_BIN_PATH}"
echo "Option {
    debug=${debug}
    port=${port}
    start=${start} 
    quit=${quit}
    mode=${mode}
    RUST_LOG=${RUST_LOG}
}"

if [ "$start" = true ]; then
    # pkill -9 
    sleep 1s
    echo "============STARTING Server============="
    $SERVER_BIN_PATH db-server -a 127.0.0.1:27023 -u root -p root -s 127.0.0.1:3306 -d mysql >$TARGET_FOLDER/dbserver.out &
    sleep 2s
    echo "=================ENDING================="
fi


if [ "$quit" = true ]; then
    echo "kill all processes for server"
    pkill -9 db-server
fi
