package com.tw.apps

import org.apache.spark.scheduler._

class CloudWatchSparkListener(cloudWatchMetricsUtil: CloudWatchMetricsUtil) extends SparkListener {

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val metrics = taskEnd.taskMetrics
    cloudWatchMetricsUtil.pushCountMetric("task.recordsWritten", metrics.outputMetrics.recordsWritten)
  }

  // more methods to get metrics
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {}

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {}

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {}

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {}

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {}

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {}

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {}

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {}

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {}

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {}

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {}

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {}

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {}

  override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {}

  override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {}

  override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = {}

  override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = {}

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {}

  override def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = {}

  override def onOtherEvent(event: SparkListenerEvent): Unit = {}
}
