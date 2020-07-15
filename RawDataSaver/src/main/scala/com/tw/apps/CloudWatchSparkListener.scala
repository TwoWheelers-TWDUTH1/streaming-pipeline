// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.tw.apps

import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.JavaConversions
import scala.io.Source
import scala.util.parsing.json.JSON

class CloudWatchSparkListener(appName: String, jobFlowFilePath: String, cloudWatchClient: AmazonCloudWatch) extends StreamingQueryListener {

  val jobFlowId: String = getJobFlowId
  val log: Logger = Logger.getLogger(getClass.getName)

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    log.info("Cloudwatch Streaming Listener, onQueryStarted: " + appName)
    pushMetric("is_app_running", 1, StandardUnit.Count)
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    log.info("Cloudwatch Streaming Listener, onQueryProgress: " + appName)
    pushMetric("is_app_running", 1, StandardUnit.Count)
    if ( !event.progress.inputRowsPerSecond.isNaN ) pushMetric("inputRowsPerSecond", event.progress.inputRowsPerSecond, StandardUnit.Count)
    if ( !event.progress.processedRowsPerSecond.isNaN ) pushMetric("processedRowsPerSecond", event.progress.processedRowsPerSecond, StandardUnit.Count)
    pushMetric("numberOfInputRows", event.progress.numInputRows, StandardUnit.Count)
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    log.info("Cloudwatch Streaming Listener, onQueryTerminated: " + appName)
    pushMetric("is_app_running", 0, StandardUnit.Count)
  }

  def getJobFlowId: String = {
    val source = Source.fromFile(jobFlowFilePath)
    try {
      val fileContent = JSON.parseFull(source.mkString)
        .getOrElse(Map[String, Any]().empty).asInstanceOf[Map[String, Any]]

      fileContent
        .get("jobFlowId").asInstanceOf[Option[String]].getOrElse("couldn't parse jobFlowId")
    } finally {
      source.close
    }
  }

  def pushMetric(metricName: String, value: Double, unit: StandardUnit): Unit = {
    val dimensionAppName = new Dimension()
      .withName("ApplicationName")
      .withValue(appName)

    val dimensionJobFlowId = new Dimension()
      .withName("JobFlowId")
      .withValue(jobFlowId)

    val datum = new MetricDatum()
      .withMetricName(metricName)
      .withUnit(unit)
      .withValue(value)
      .withDimensions(JavaConversions.setAsJavaSet(
        Set(dimensionAppName, dimensionJobFlowId)))

    val request = new PutMetricDataRequest()
      .withNamespace("AWS/ElasticMapReduce")
      .withMetricData(datum)

    val response = cloudWatchClient.putMetricData(request)
    if (response.getSdkHttpMetadata.getHttpStatusCode != 200) {
      log.warn("Failed pushing CloudWatch Metric with RequestId: " + response.getSdkResponseMetadata.getRequestId)
      log.debug("Response Status code: " + response.getSdkHttpMetadata.getHttpStatusCode)
    }
  }
}
