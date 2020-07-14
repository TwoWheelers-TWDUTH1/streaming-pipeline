#!/usr/bin/env bash

if [ $# -eq 0 ]; then
    usage
    exit 1
fi

TEST_BASTION_PUBLIC_IP=$1
TRAINING_COHORT=$2

TEST_EMR="emr-master.${TRAINING_COHORT}.training"

echo "====Updating SSH Config===="

echo "
	User ec2-user
	IdentitiesOnly yes
	ForwardAgent yes
	DynamicForward 6789
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null

Host ${TEST_EMR}
    User hadoop

Host *.${TRAINING_COHORT}.training !bastion.${TRAINING_COHORT}.training
	ForwardAgent yes
	ProxyCommand ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ec2-user@${TEST_BASTION_PUBLIC_IP} -W %h:%p 2>/dev/null
	User ec2-user
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null

Host bastion.${TRAINING_COHORT}.training
    User ec2-user
    HostName ${TEST_BASTION_PUBLIC_IP}
    DynamicForward 6789
" >> ~/.ssh/config

echo "====SSH Config Updated===="

echo "====Insert app config in MSK zookeeper===="
scp ./zookeeper/seed.sh ${TEST_EMR}:/tmp/zookeeper-seed.sh

ssh ${TEST_EMR} <<EOF
set -e
zk_broker_list=\$(aws kafka list-clusters | jq .ClusterInfoList[0].ZookeeperConnectString -r)
emr_arn=\$(aws kafka list-clusters | jq .ClusterInfoList[0].ClusterArn -r)
export hdfs_server="${TEST_EMR}:8020"
export kafka_server="\$(aws kafka get-bootstrap-brokers --cluster-arn "\$(emr_arn)" | jq .BootstrapBrokerStringTls -r)"
export zk_command="/home/ec2-user/kafka_2.11-1.1.1/bin/zookeeper-shell \${zk_broker_list}"
sh /tmp/zookeeper-seed.sh
EOF

echo "====Inserted app config in zookeeper===="

ssh ingester.${TRAINING_COHORT}.training <<EOF
set -e

function kill_process {
    query=\$1
    pid=`ps aux | grep \$query | grep -v "grep" |  awk "{print \\\$2}"`

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
nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=\${station_information} --kafka.brokers=${kafka_server} --kafka.security.protocol=SSL 1>/tmp/\${station_information}-msk.log 2>/tmp/\${station_information}-msk.error.log &
nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=\${station_san_francisco} --kafka.brokers=${kafka_server} --kafka.security.protocol=SSL 1>/tmp/\${station_san_francisco}-msk.log 2>/tmp/\${station_san_francisco}-msk.error.log &
nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=\${station_status} --kafka.brokers=${kafka_server} --kafka.security.protocol=SSL 1>/tmp/\${station_status}-msk.log 2>/tmp/\${station_status}-msk.error.log &
EOF

echo "====Configure HDFS paths===="
scp ./hdfs/seed.sh ${TEST_EMR}:/tmp/hdfs-seed.sh

ssh ${TEST_EMR} <<EOF
set -e
export hdfs_server="${TEST_EMR}:8020"
export hadoop_path="hadoop"
sh /tmp/hdfs-seed.sh
EOF

echo "====HDFS paths configured==="
