package com.tw.apps

import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import org.apache.log4j.Logger

import scala.collection.JavaConversions
import scala.io.Source
import scala.util.parsing.json.JSON

class CloudWatchMetricsUtil(appName: String, jobFlowFilePath: String = "/mnt/var/lib/info/job-flow.json", cloudWatchClient: AmazonCloudWatch = AmazonCloudWatchClientBuilder.defaultClient()) {

  private val log: Logger = Logger.getLogger(getClass.getName)
  private lazy val jobFlowId: String = getJobFlowId

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

  def pushCountMetric(metricName: String, recordsWritten: Long): Unit = pushMetric(metricName, recordsWritten, StandardUnit.Count)

  private def pushMetric(metricName: String, value: Double, unit: StandardUnit): Unit = {
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
