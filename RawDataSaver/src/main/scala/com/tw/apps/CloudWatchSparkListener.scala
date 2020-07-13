// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.tw.apps

import java.util

import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

class CloudWatchSparkListener(appName: String = "ApplicationName") extends StreamingQueryListener {

  val log: Logger = Logger.getLogger(getClass.getName)
  val dimensionsMap: mutable.Map[String, String] = mutable.HashMap[String, String]()
  val cw: AmazonCloudWatch = AmazonCloudWatchClientBuilder.defaultClient()
  val jobFlowInfoFile = "/mnt/var/lib/info/job-flow.json"
  val jobFlowId: String = parseJsonWithJackson(Source.fromFile(jobFlowInfoFile)).get("jobFlowId").mkString

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    log.info("Cloudwatch Streaming Listener, onQueryStarted: " + appName)
    pushMetric(dimensionsMap, "is_app_running", 1, StandardUnit.Count)
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    log.info("Cloudwatch Streaming Listener, onQueryProgress: " + appName)
    pushMetric(dimensionsMap, "is_app_running", 1, StandardUnit.Count)
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    log.info("Cloudwatch Streaming Listener, onQueryTerminated: " + appName)
    pushMetric(dimensionsMap, "is_app_running", 0, StandardUnit.Count)
  }

  def parseJsonWithJackson(json: BufferedSource): mutable.Map[String, Object] = {
    val attrMapper = new ObjectMapper() with ScalaObjectMapper
    attrMapper.registerModule(DefaultScalaModule)
    attrMapper.readValue[mutable.Map[String, Object]](json.reader())
  }

  def pushMetric(dimensionItems: mutable.Map[String, String], metricName: String, value: Double, unit: StandardUnit) {
    val dimensions = new util.ArrayList[Dimension]()

    for ((k, v) <- dimensionItems) {
      val dimension = new Dimension().withName(k).withValue(v)
      dimensions.add(dimension)
    }

    val dimensionAppName = new Dimension()
      .withName("ApplicationName")
      .withValue(appName)

    dimensions.add(dimensionAppName)

    val dimentionsJobFlowId = new Dimension()
      .withName("JobFlowId")
      .withValue(jobFlowId)

    dimensions.add(dimentionsJobFlowId)

    val datum = new MetricDatum()
      .withMetricName(metricName)
      .withUnit(unit)
      .withValue(value)
      .withDimensions(dimensions)

    val request = new PutMetricDataRequest()
      .withNamespace("AWS/ElasticMapReduce")
      .withMetricData(datum)

    val response = cw.putMetricData(request)
    if (response.getSdkHttpMetadata.getHttpStatusCode != 200) {
      log.warn("Failed pushing CloudWatch Metric with RequestId: " + response.getSdkResponseMetadata.getRequestId)
      log.debug("Response Status code: " + response.getSdkHttpMetadata.getHttpStatusCode)
    }
  }
}
