#!/usr/bin/env bash

if [ $# -eq 0 ]; then
  usage
  exit 1
fi

BASTION_PUBLIC_IP=$1
TRAINING_COHORT=$2

echo "====Updating SSH Config===="

echo "
	User ec2-user
	IdentitiesOnly yes
	ForwardAgent yes
	DynamicForward 6789
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null

Host emr-master.${TRAINING_COHORT}.training
    User hadoop

Host *.${TRAINING_COHORT}.training !bastion.${TRAINING_COHORT}.training
	ForwardAgent yes
	ProxyCommand ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ec2-user@${BASTION_PUBLIC_IP} -W %h:%p 2>/dev/null
	User ec2-user
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null

Host bastion.${TRAINING_COHORT}.training
    User ec2-user
    HostName ${BASTION_PUBLIC_IP}
    DynamicForward 6789
" >>~/.ssh/config

ssh-add ~/.ssh/id_rsa_*

echo "====SSH Config Updated===="

echo "====Insert app config in MSK zookeeper===="
scp ./zookeeper/seed.sh emr-master.${TRAINING_COHORT}.training:/tmp/zookeeper-seed.sh

emr_arn=$(
  ssh emr-master.${TRAINING_COHORT}.training <<EOF
aws kafka list-clusters | jq .ClusterInfoList[0].ClusterArn -r)
EOF
)

zk_broker_list=$(
  ssh emr-master.${TRAINING_COHORT}.training <<EOF
aws kafka list-clusters | jq .ClusterInfoList[0].ZookeeperConnectString -r)
EOF
)

kafka_server=$(
  ssh emr-master.${TRAINING_COHORT}.training <<EOF
aws kafka get-bootstrap-brokers --cluster-arn "${emr_arn}" | jq .BootstrapBrokerStringTls -r
EOF
)

ssh emr-master.${TRAINING_COHORT}.training <<EOF
set -e
export hdfs_server="emr-master.${TRAINING_COHORT}.training:8020"
export kafka_server="${kafka_server}"
export zk_command="zookeeper-client -server ${zk_broker_list}"
sh /tmp/zookeeper-seed.sh
EOF

echo "====Inserted app config in zookeeper===="

ssh ingester.${TRAINING_COHORT}.training <<EOF
set -e

function kill_process {
    query=\$1
    pid=\$(ps aux | grep \$query | grep -v "grep" | grep "SSL" | awk "{print \\\$2}")

    if [ -z "\$pid" ];
    then
        echo "no \${query} process running"
    else
        kill -9 \$pid
    fi
}

station_information="station-information"
station_status="station-status"
station_san_francisco="station-san-francisco"


echo "====Kill running producers===="

kill_process \${station_information}
kill_process \${station_status}
kill_process \${station_san_francisco}

echo "====Runing Producers Killed===="

echo "====Deploy Producers => MSK===="
nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=\${station_information} --kafka.brokers=${kafka_server} --application.name=StationInformationIngesterMsk --kafka.security.protocol=SSL 1>/tmp/\${station_information}-msk.log 2>/tmp/\${station_information}-msk.error.log &
nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=\${station_san_francisco} --kafka.brokers=${kafka_server}  --application.name=StationSFIngesterMsk --kafka.security.protocol=SSL 1>/tmp/\${station_san_francisco}-msk.log 2>/tmp/\${station_san_francisco}-msk.error.log &
nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=\${station_status} --kafka.brokers=${kafka_server} --application.name=StationStatusIngesterMsk --kafka.security.protocol=SSL 1>/tmp/\${station_status}-msk.log 2>/tmp/\${station_status}-msk.error.log &
EOF

echo "====Configure HDFS paths===="
scp ./hdfs/seed.sh emr-master.${TRAINING_COHORT}.training:/tmp/hdfs-seed.sh

ssh emr-master.${TRAINING_COHORT}.training <<EOF
set -e
export hdfs_server="emr-master.${TRAINING_COHORT}.training:8020"
export hadoop_path="hadoop"
sh /tmp/hdfs-seed.sh
EOF

echo "====HDFS paths configured==="

echo "====Copy Raw Data Saver Jar to EMR===="
scp RawDataSaver/target/scala-2.11/tw-raw-data-saver_2.11-0.0.1.jar emr-master.${TRAINING_COHORT}.training:/tmp/
echo "====Raw Data Saver Jar Copied to EMR===="

scp sbin/go.sh emr-master.${TRAINING_COHORT}.training:/tmp/go.sh

ssh emr-master.${TRAINING_COHORT}.training <<EOF
set -e

source /tmp/go.sh

echo "====Kill Old Raw Data Saver===="

kill_application "StationStatusSaverAppMsk"
kill_application "StationInformationSaverAppMsk"
kill_application "StationDataSFSaverAppMsk"

echo "====Old Raw Data Saver Killed===="

echo "====Deploy Raw Data Saver===="

nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationStatusSaverAppMsk --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=500m --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar "${zk_broker_list}" "/tw/stationStatus" SSL 1>/tmp/raw-station-status-data-saver-msk.log 2>/tmp/raw-station-status-data-saver-msk.error.log &
nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationInformationSaverAppMsk --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=500m --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar "${zk_broker_list}" "/tw/stationInformation" SSL 1>/tmp/raw-station-information-data-saver-msk.log 2>/tmp/raw-station-information-data-saver-msk.error.log &
nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationDataSFSaverAppMsk --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=500m --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar "${zk_broker_list}" "/tw/stationDataSF" SSL 1>/tmp/raw-station-data-sf-saver-msk.log 2>/tmp/raw-station-data-sf-saver-msk.error.log &

echo "====Raw Data Saver Deployed===="
EOF
