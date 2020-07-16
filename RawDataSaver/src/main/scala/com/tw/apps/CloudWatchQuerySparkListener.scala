package com.tw.apps

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

class CloudWatchQuerySparkListener(appName: String, cloudWatchMetricsUtil: CloudWatchMetricsUtil) extends StreamingQueryListener {

  val log: Logger = Logger.getLogger(getClass.getName)

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    log.info("Cloudwatch Streaming Listener, onQueryStarted: " + appName)
    cloudWatchMetricsUtil.pushCountMetric("is_app_running", 1)
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    log.info("Cloudwatch Streaming Listener, onQueryProgress: " + appName)
    cloudWatchMetricsUtil.pushCountMetric("is_app_running", 1)
    if ( !event.progress.inputRowsPerSecond.isNaN )
      cloudWatchMetricsUtil.pushCountMetric("inputRowsPerSecond", event.progress.inputRowsPerSecond.toLong)
    if ( !event.progress.processedRowsPerSecond.isNaN )
      cloudWatchMetricsUtil.pushCountMetric("processedRowsPerSecond", event.progress.processedRowsPerSecond.toLong)
    cloudWatchMetricsUtil.pushCountMetric("numberOfInputRows", event.progress.numInputRows)
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    log.info("Cloudwatch Streaming Listener, onQueryTerminated: " + appName)
    event.exception.foreach(e => log.error("Cloudwatch Streaming Listener terminated because of error: " + e))
    cloudWatchMetricsUtil.pushCountMetric("is_app_running", 0)
  }
}
