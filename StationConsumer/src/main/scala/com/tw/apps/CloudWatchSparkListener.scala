// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.tw.apps

import java.util

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import org.apache.log4j.Logger
import org.apache.spark.streaming.scheduler.{StreamingListener, _}

import scala.collection.mutable.{HashMap, Map}

class CloudWatchSparkListener(appName: String = "ApplicationName") extends StreamingListener {

  val log = Logger.getLogger(getClass.getName)
  val dimensionsMap = new HashMap[String,String]()
  val cw = AmazonCloudWatchClientBuilder.defaultClient()

  /**
    * This method executes when a Spark Streaming batch completes.
    *
    * @param batchCompleted Class having information on the completed batch
    */

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    log.info("CloudWatch Streaming Listener, onBatchCompleted:" + appName)

    // write performance metrics to CloutWatch Metrics
    writeBatchStatsToCloudWatch(batchCompleted)

  }
  /**
    * This method executes when a Spark Streaming batch completes.
    *
    * @param receiverError Class having information on the reciever Errors
    */

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    log.warn("CloudWatch Streaming Listener, onReceiverError:" + appName)

    writeRecieverStatsToCloudWatch(receiverError)
  }

  /**
    * This method will just send one, whenever there is a recieverError
    * @param receiverError Class having information on the completed batch
    */
  def writeRecieverStatsToCloudWatch(receiverError: StreamingListenerReceiverError): Unit = {

    sendHeartBeat(dimensionsMap,"receiverError")

  }

  def sendHeartBeat(dimensionItems : Map[String,String], metricName: String) : Unit = {
    pushMetric(dimensionItems,"heartBeat",1.0, StandardUnit.Count)
  }

  def pushMetric(dimensionItems : Map[String,String], metricName: String, value : Double, unit: StandardUnit ) {
    val dimentions = new util.ArrayList[Dimension]()

    for ((k,v) <- dimensionItems) {
      var dimension = new Dimension().withName(k).withValue(v)
      dimentions.add(dimension)
    }

    var dimensionAppName = new Dimension()
      .withName("ApplicationName")
      .withValue(appName)

    dimentions.add(dimensionAppName)

    var dimentionsJobFlowId = new Dimension()
      .withName("JobFlowId")
      .withValue("not sure yet")

    dimentions.add(dimentionsJobFlowId)

    var datum = new MetricDatum()
      .withMetricName(metricName)
      .withUnit(unit)
      .withValue(value)
      .withDimensions(dimentions)

    var request = new PutMetricDataRequest()
      .withNamespace("AWS/ElasticMapReduce")
      .withMetricData(datum)

    val response = cw.putMetricData(request)
    if (response.getSdkHttpMetadata.getHttpStatusCode != 200) {
      log.warn("Failed pushing CloudWatch Metric with RequestId: " + response.getSdkResponseMetadata.getRequestId)
      log.debug("Response Status code: " + response.getSdkHttpMetadata.getHttpStatusCode)
    }
  }

  def writeBatchStatsToCloudWatch(batchCompleted: StreamingListenerBatchCompleted): Unit = {

    val processingTime = if (batchCompleted.batchInfo.processingDelay.isDefined) {
      batchCompleted.batchInfo.processingDelay.get
    }

    val schedulingDelay = if (batchCompleted.batchInfo.schedulingDelay.isDefined && batchCompleted.batchInfo.schedulingDelay.get > 0 ) {
      batchCompleted.batchInfo.schedulingDelay.get
    } else { 0 }

    val numRecords = batchCompleted.batchInfo.numRecords
      sendHeartBeat(dimensionsMap,"batchCompleted")
//    metricUtil.pushMillisecondsMetric(dimentionsMap, "schedulingDelay", schedulingDelay )
//    metricUtil.pushMillisecondsMetric(dimentionsMap, "processingDelay", batchCompleted.batchInfo.processingDelay.get )
//    metricUtil.pushCountMetric(dimentionsMap, "numRecords", numRecords)
//    metricUtil.pushMillisecondsMetric(dimentionsMap, "totalDelay", batchCompleted.batchInfo.totalDelay.get);

    log.info("Batch completed at: " + batchCompleted.batchInfo.processingEndTime.get +
      " was started at: " + batchCompleted.batchInfo.processingStartTime.get +
      " submission time: " + batchCompleted.batchInfo.submissionTime +
      " batch time: " + batchCompleted.batchInfo.batchTime +
      " batch processing delay: " + batchCompleted.batchInfo.processingDelay.get +
      " records : " + numRecords +
      " total batch delay:" + batchCompleted.batchInfo.totalDelay.get +
      " product prefix:" + batchCompleted.batchInfo.productPrefix +
      " schedulingDelay:" + schedulingDelay +
      " processingTime:" + processingTime
    )
  }
}
