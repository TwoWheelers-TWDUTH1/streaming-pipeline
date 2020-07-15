package com.tw.apps

import java.io.FileInputStream

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import org.apache.spark.sql.SparkSession
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Duration, StreamingContext}

object StationLocationApp {
  def main(args: Array[String]): Unit = {

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    if (args.length != 2) {
      val message = "Two arguments are required: \"zookeeper server\" and \"application folder in zookeeper\"!"
      throw new IllegalArgumentException(message)
    }
    val zookeeperConnectionString = args(0)

    val zookeeperFolder = args(1)

    val securityProtocal = args(2)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start

    val kafkaBrokers = new String(zkClient.getData.forPath(s"$zookeeperFolder/kafkaBrokers"))

    val topic = new String(zkClient.getData.watched.forPath(s"$zookeeperFolder/topic"))

    val checkpointLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/checkpointLocation"))

    val dataLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/dataLocation"))


    val spark = SparkSession.builder
      .getOrCreate()

    val runtimeAppName = spark.sparkContext.appName
    val filePath = "/mnt/var/lib/info/job-flow.json"
    val cwListener = new CloudWatchSparkListener(runtimeAppName, filePath, AmazonCloudWatchClientBuilder.defaultClient())
    spark.streams.addListener(cwListener)

    val savedStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", false)
      .option("security.protocol", securityProtocal)
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .withColumn("date", date_format(current_date(), "yyyy-MM-dd"))
      .writeStream
      .partitionBy("date")
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", checkpointLocation)
      .option("path", dataLocation)
      .start()
      .awaitTermination()
  }
}
