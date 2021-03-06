#!/usr/bin/env bash
set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "====Building Producer JARs===="
$DIR/../CitibikeApiProducer/gradlew -p $DIR/../CitibikeApiProducer clean test bootJar
echo "====Building Consumer JARs===="
cd $DIR/../RawDataSaver && sbt test && sbt package
cd $DIR/../StationConsumer && sbt test && sbt package
cd $DIR/../StationTransformerNYC && sbt test && sbt package
cd $DIR/../SmokeTest && sbt test && sbt package
