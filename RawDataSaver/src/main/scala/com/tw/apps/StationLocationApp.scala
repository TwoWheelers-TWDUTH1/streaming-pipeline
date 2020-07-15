package com.tw.apps

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StationLocationApp {
  def main(args: Array[String]): Unit = {

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    if (args.length < 2) {
      val message = "At least two arguments are required: " +
        "\"zookeeper server\" (requried), \"application folder in zookeeper\" (required), " +
        "and kafka security protocol (optional)!"
      throw new IllegalArgumentException(message)
    }
    val zookeeperConnectionString = args(0)

    val zookeeperFolder = args(1)

    val securityProtocol = if (args.length < 3) "PLAINTEXT" else args(2)

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
      .option("kafka.security.protocol", securityProtocol)
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
