##!/usr/bin/env bash
#
#set -xe
#
#usage() {
#    echo "Deployment script "
#    echo "./deploy.sh BASTION_PUBLIC_IP TRAINING_COHORT"
#}
#
#if [ $# -eq 0 ]; then
#    usage
#    exit 1
#fi
#
#BASTION_PUBLIC_IP=$1
#TRAINING_COHORT=$2
#
#echo "====Updating SSH Config===="
#
#echo "
#	User ec2-user
#	IdentitiesOnly yes
#	ForwardAgent yes
#	DynamicForward 6789
#    StrictHostKeyChecking no
#    UserKnownHostsFile /dev/null
#
#Host emr-master.${TRAINING_COHORT}.training
#    User hadoop
#
#Host *.${TRAINING_COHORT}.training !bastion.${TRAINING_COHORT}.training
#	ForwardAgent yes
#	ProxyCommand ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ec2-user@${BASTION_PUBLIC_IP} -W %h:%p 2>/dev/null
#	User ec2-user
#    StrictHostKeyChecking no
#    UserKnownHostsFile /dev/null
#
#Host bastion.${TRAINING_COHORT}.training
#    User ec2-user
#    HostName ${BASTION_PUBLIC_IP}
#    DynamicForward 6789
#" >> ~/.ssh/config
#
#ssh-add ~/.ssh/id_rsa_*
#
#echo "====SSH Config Updated===="
#
#echo "====Insert app config in zookeeper===="
#scp ./zookeeper/seed.sh kafka.${TRAINING_COHORT}.training:/tmp/zookeeper-seed.sh
#ssh kafka.${TRAINING_COHORT}.training <<EOF
#set -e
#export hdfs_server="emr-master.${TRAINING_COHORT}.training:8020"
#export kafka_server="kafka.${TRAINING_COHORT}.training:9092"
#export zk_command="zookeeper-shell localhost:2181"
#sh /tmp/zookeeper-seed.sh
#
#/bin/kafka-topics --zookeeper localhost:2181 --create --if-not-exists --partitions 1 --replication-factor 1 --topic station_data_nyc
#/bin/kafka-configs --entity-type topics --zookeeper localhost:2181 --add-config retention.ms=18000000 --alter --entity-name station_data_nyc
#
#/bin/kafka-topics --zookeeper localhost:2181 --create --if-not-exists --partitions 1 --replication-factor 1 --topic station_data_sf
#/bin/kafka-configs --entity-type topics --zookeeper localhost:2181 --add-config retention.ms=18000000 --alter --entity-name station_data_sf
#
#/bin/kafka-topics --zookeeper localhost:2181 --create --if-not-exists --partitions 1 --replication-factor 1 --topic station_data_marseille
#/bin/kafka-configs --entity-type topics --zookeeper localhost:2181 --add-config retention.ms=18000000 --alter --entity-name station_data_marseille
#
#/bin/kafka-topics --zookeeper localhost:2181 --create --if-not-exists --partitions 1 --replication-factor 1 --topic station_information
#/bin/kafka-configs --entity-type topics --zookeeper localhost:2181 --add-config retention.ms=18000000 --alter --entity-name station_information
#
#/bin/kafka-topics --zookeeper localhost:2181 --create --if-not-exists --partitions 1 --replication-factor 1 --topic station_status
#/bin/kafka-configs --entity-type topics --zookeeper localhost:2181 --add-config retention.ms=18000000 --alter --entity-name station_status
#
#/bin/kafka-topics --zookeeper localhost:2181 --create --if-not-exists --partitions 1 --replication-factor 1 --topic station_data_marseille
#/bin/kafka-configs --entity-type topics --zookeeper localhost:2181 --add-config retention.ms=18000000 --alter --entity-name station_data_marseille
#EOF
#
#echo "====Inserted app config in zookeeper===="
#
#echo "====Copy jar to ingester server===="
#scp CitibikeApiProducer/build/libs/tw-citibike-apis-producer0.1.0.jar ingester.${TRAINING_COHORT}.training:/tmp/
#echo "====Jar copied to ingester server===="
#
#ssh ingester.${TRAINING_COHORT}.training <<EOF
#set -e
#
#function kill_process {
#    query=\$1
#    pid=\$(ps aux | grep \$query | grep -v "grep" | grep -v "SSL" |  awk "{print \\\$2}")
#
#    if [ -z "\$pid" ];
#    then
#        echo "no \${query} process running"
#    else
#        kill -9 \$pid
#    fi
#}
#
#station_information="station-information"
#station_status="station-status"
#station_san_francisco="station-san-francisco"
#station_marseille="station-marseille"
#
#
#echo "====Kill running producers===="
#
#kill_process \${station_information}
#kill_process \${station_status}
#kill_process \${station_san_francisco}
#kill_process \${station_marseille}
#
#echo "====Runing Producers Killed===="
#
#echo "====Deploy Producers===="
#nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=\${station_information} --application.name=StationInformationIngester --kafka.brokers=kafka.${TRAINING_COHORT}.training:9092 1>/tmp/\${station_information}.log 2>/tmp/\${station_information}.error.log &
#nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=\${station_san_francisco} --application.name=StationSFIngester --producer.topic=station_data_sf --kafka.brokers=kafka.${TRAINING_COHORT}.training:9092 1>/tmp/\${station_san_francisco}.log 2>/tmp/\${station_san_francisco}.error.log &
#nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=\${station_status} --application.name=StationStatusIngester --kafka.brokers=kafka.${TRAINING_COHORT}.training:9092 1>/tmp/\${station_status}.log 2>/tmp/\${station_status}.error.log &
#nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=\${station_marseille} --application.name=StationSFIngester --producer.topic=station_data_sf --kafka.brokers=kafka.${TRAINING_COHORT}.training:9092 1>/tmp/\${station_marseille}.log 2>/tmp/\${station_marseille}.error.log &
#
#echo "====Producers Deployed===="
#EOF
#
#
#echo "====Configure HDFS paths===="
#scp ./hdfs/seed.sh emr-master.${TRAINING_COHORT}.training:/tmp/hdfs-seed.sh
#
#ssh emr-master.${TRAINING_COHORT}.training <<EOF
#set -e
#export hdfs_server="emr-master.${TRAINING_COHORT}.training:8020"
#export hadoop_path="hadoop"
#sh /tmp/hdfs-seed.sh
#EOF
#
#echo "====HDFS paths configured==="
#
#
#echo "====Copy Raw Data Saver Jar to EMR===="
#scp RawDataSaver/target/scala-2.11/tw-raw-data-saver_2.11-0.0.1.jar emr-master.${TRAINING_COHORT}.training:/tmp/
#echo "====Raw Data Saver Jar Copied to EMR===="
#
#scp sbin/go.sh emr-master.${TRAINING_COHORT}.training:/tmp/go.sh
#
#ssh emr-master.${TRAINING_COHORT}.training <<EOF
#set -e
#
#source /tmp/go.sh
#
#echo "====Kill Old Raw Data Saver===="
#
#kill_application "StationStatusSaverApp"
#kill_application "StationInformationSaverApp"
#kill_application "StationDataSFSaverApp"
#kill_application "StationDataMarseilleSaverApp"
#
#echo "====Old Raw Data Saver Killed===="
#
#echo "====Deploy Raw Data Saver===="
#
#nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationStatusSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=500m --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar kafka.${TRAINING_COHORT}.training:2181 "/tw/stationStatus" PLAINTEXT 1>/tmp/raw-station-status-data-saver.log 2>/tmp/raw-station-status-data-saver.error.log &
#
#nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationInformationSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=500m --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar kafka.${TRAINING_COHORT}.training:2181 "/tw/stationInformation" PLAINTEXT 1>/tmp/raw-station-information-data-saver.log 2>/tmp/raw-station-information-data-saver.error.log &
#
#nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationDataSFSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=500m --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar kafka.${TRAINING_COHORT}.training:2181 "/tw/stationDataSF" PLAINTEXT 1>/tmp/raw-station-data-sf-saver.log 2>/tmp/raw-station-data-sf-saver.error.log &
#
#nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationDataMarseilleSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=500m --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar kafka.${TRAINING_COHORT}.training:2181 "/tw/stationDataMarseille" PLAINTEXT 1>/tmp/raw-station-data-marseille-saver.log 2>/tmp/raw-station-data-marseille-saver.error.log &
#
#echo "====Raw Data Saver Deployed===="
#EOF
#
#
#echo "====Copy Station Consumers Jar to EMR===="
#scp StationConsumer/target/scala-2.11/tw-station-consumer_2.11-0.0.1.jar emr-master.${TRAINING_COHORT}.training:/tmp/
#
#scp StationTransformerNYC/target/scala-2.11/tw-station-transformer-nyc_2.11-0.0.1.jar emr-master.${TRAINING_COHORT}.training:/tmp/
#echo "====Station Consumers Jar Copied to EMR===="
#
#echo "====Copy Data Smoke Test to EMR===="
#scp SmokeTest/target/scala-2.11/tw-data-smoke-test_2.11-0.0.1.jar emr-master.${TRAINING_COHORT}.training:/tmp/
#echo "====Data Smoke Test to EMR Copied to EMR===="
#
#scp sbin/go.sh emr-master.${TRAINING_COHORT}.training:/tmp/go.sh
#
#ssh emr-master.${TRAINING_COHORT}.training <<EOF
#set -e
#
#source /tmp/go.sh
#
#
#echo "====Kill Old Station Consumers===="
#
#kill_application "StationApp"
#kill_application "StationTransformerNYC"
#
#echo "====Old Station Consumers Killed===="
#
#echo "====Deploy Station Consumers===="
#
#nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationApp --name StationApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0  --driver-memory 500M --conf spark.executor.memory=500m --conf spark.cores.max=1 --conf spark.dynamicAllocation.maxExecutors=5 /tmp/tw-station-consumer_2.11-0.0.1.jar kafka.${TRAINING_COHORT}.training:2181 1>/tmp/station-consumer.log 2>/tmp/station-consumer.error.log &
#
#nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationApp --name StationTransformerNYC --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0  --driver-memory 500M --conf spark.executor.memory=500m --conf spark.cores.max=1 /tmp/tw-station-transformer-nyc_2.11-0.0.1.jar kafka.${TRAINING_COHORT}.training:2181 1>/tmp/station-transformer-nyc.log 2>/tmp/station-transformer-nyc.error.log &
#
#echo "====Station Consumers Deployed===="
#
#echo "====Setting Cron Jobs===="
#
#echo "*/10 * * * * hadoop /usr/bin/flock --nonblock /tmp/data-smoke-test.lock /usr/bin/spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.SmokeTest --name DataSmokeTest  --driver-memory 500M --conf spark.executor.memory=500m --conf spark.cores.max=1 /tmp/tw-data-smoke-test_2.11-0.0.1.jar   hdfs://emr-master.${TRAINING_COHORT}.training:8020/tw/stationMart/data >>/var/log/data-smoke-test.cron.log 2>&1" | sudo tee /etc/cron.d/data-smoke-test
#
#EOF
