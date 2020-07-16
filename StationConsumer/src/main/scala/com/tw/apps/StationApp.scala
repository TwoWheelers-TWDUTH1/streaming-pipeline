package com.tw.apps

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.tw.apps.StationDataTransformation._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.SparkSession

object StationApp {

  def main(args: Array[String]): Unit = {

    val zookeeperConnectionString = if (args.isEmpty) "zookeeper:2181" else args(0)

    val securityProtocol = if (args.length < 2) "PLAINTEXT" else args(1)

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start()

    val stationKafkaBrokers = new String(zkClient.getData.forPath("/tw/stationStatus/kafkaBrokers"))

    val nycStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataNYC/topic"))
    val sfStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataSF/topic"))
    val marseilleStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataMarseille/topic"))

    val checkpointLocation = new String(
      zkClient.getData.watched.forPath("/tw/output/checkpointLocation"))

    val outputLocation = new String(
      zkClient.getData.watched.forPath("/tw/output/dataLocation"))


    val spark = SparkSession.builder
      .getOrCreate()

    val runtimeAppName = spark.sparkContext.appName
    val filePath = "/mnt/var/lib/info/job-flow.json"
    val cwListener = new CloudWatchSparkListener(runtimeAppName, filePath, AmazonCloudWatchClientBuilder.defaultClient())
    spark.streams.addListener(cwListener)

    import spark.implicits._

    val nycStationDF = loadKafkaJsonStream(
      securityProtocol,
      stationKafkaBrokers,
      nycStationTopic,
      spark
    ).transform(nycStationStatusJson2DF(_, spark))

    val sfStationDF = loadKafkaJsonStream(
      securityProtocol,
      stationKafkaBrokers,
      sfStationTopic,
      spark
    ).transform(citybikeV2StationStatusJson2DF(_, spark))

    val marseilleStationDF = loadKafkaJsonStream(
      securityProtocol,
      stationKafkaBrokers,
      marseilleStationTopic,
      spark
    ).transform(citybikeV2StationStatusJson2DF(_, spark))

    nycStationDF
      .union(sfStationDF)
      .union(marseilleStationDF)
      .as[StationData]
      .groupByKey(r=>r.station_id)
      .reduceGroups((r1,r2)=>if (r1.last_updated > r2.last_updated) r1 else r2)
      .map(_._2)
      .writeStream
      .format("overwriteCSV")
      .outputMode("complete")
      .option("header", true)
      .option("truncate", false)
      .option("checkpointLocation", checkpointLocation)
      .option("path", outputLocation)
      .start()
      .awaitTermination()

  }

  private def loadKafkaJsonStream(securityProtocol: String, stationKafkaBrokers: String, topic: String, spark: SparkSession) = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", stationKafkaBrokers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("auto.offset.reset", "smallest")
      .option("kafka.security.protocol", securityProtocol)
      .option("failOnDataLoss", true)
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
  }
}
