#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd ${DIR}
TOPIC="$1"

if [ -z "${TOPIC}" ]; then
  echo "Usage: $0 <topic>"
  exit 1
fi

./docker-compose.sh exec kafka kafka-console-consumer.sh --zookeeper zookeeper:2181 --topic station_data_sf
