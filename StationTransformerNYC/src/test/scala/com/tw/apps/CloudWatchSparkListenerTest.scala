package com.tw.apps

import com.amazonaws.http.{HttpResponse, SdkHttpMetadata}
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.{PutMetricDataRequest, PutMetricDataResult}
import org.apache.spark.sql.SparkSession
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.MockFactory
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConversions._

class CloudWatchSparkListenerTest extends AnyFeatureSpec with Matchers with MockFactory {
  val jobFlowFilePath = getClass.getResource("/job-flow.json").getPath

  Feature("Extract information from Job Flow file") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()

    Scenario("Parse job-flow id") {
      val listener = new CloudWatchSparkListener("some-app", jobFlowFilePath, mock[AmazonCloudWatch])

      listener.getJobFlowId should be("j-XN2TG35DXXN")
    }
  }

  Feature("Metrics are pushed to CloudWatch") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()

    Scenario("onQueryStarted should send a is_app_running with value = 1") {
      val cloudWatchClientMock = mock[AmazonCloudWatch]
      val listener = new CloudWatchSparkListener("some-app", jobFlowFilePath, cloudWatchClientMock)

      val putMetricRequestCapture = mockAndCapturePutMetricInteraction(cloudWatchClientMock, 200)
      listener.onQueryStarted(null)

      putMetricRequestCapture.value.getNamespace should be("AWS/ElasticMapReduce")
      val metricData = putMetricRequestCapture.value.getMetricData.toList
      metricData.filter(d => "is_app_running".equals(d.getMetricName)).head.getValue should be(1.0d)
    }

    ignore("onQueryProgress should send a is_app_running with value = 1") {
      val cloudWatchClientMock = mock[AmazonCloudWatch]
      val listener = new CloudWatchSparkListener("some-app", jobFlowFilePath, cloudWatchClientMock)

      val putMetricRequestCapture = mockAndCapturePutMetricInteraction(cloudWatchClientMock, 200)
      listener.onQueryProgress(null)

      putMetricRequestCapture.value.getNamespace should be("AWS/ElasticMapReduce")
      val metricData = putMetricRequestCapture.value.getMetricData.toList
      metricData.filter(d => "is_app_running".equals(d.getMetricName)).head.getValue should be(1.0d)
    }

    Scenario("onQueryTerminated should send a is_app_running with value = 0") {
      val cloudWatchClientMock = mock[AmazonCloudWatch]
      val listener = new CloudWatchSparkListener("some-app", jobFlowFilePath, cloudWatchClientMock)

      val putMetricRequestCapture = mockAndCapturePutMetricInteraction(cloudWatchClientMock, 200)
      listener.onQueryTerminated(null)

      putMetricRequestCapture.value.getNamespace should be("AWS/ElasticMapReduce")
      val metricData = putMetricRequestCapture.value.getMetricData.toList
      metricData.filter(d => "is_app_running".equals(d.getMetricName)).head.getValue should be(0.0d)
    }
  }

  private def mockAndCapturePutMetricInteraction(cloudWatchClientMock: _root_.com.amazonaws.services.cloudwatch.AmazonCloudWatch, responseCode: Int): CaptureOne[PutMetricDataRequest] = {
    val sdkHttpMetadataMock = SdkHttpMetadata.from(
      new HttpResponse(null, null) {
        override def getStatusCode: Int = responseCode
      })
    val putMetricResultMock = mock[PutMetricDataResult]
    (putMetricResultMock.getSdkHttpMetadata _).expects().onCall(() => sdkHttpMetadataMock).atLeastOnce()
    val putMetricRequestCapture = CaptureOne[PutMetricDataRequest]()
    cloudWatchClientMock.putMetricData _ expects capture(putMetricRequestCapture) onCall { p: PutMetricDataRequest => putMetricResultMock } once

    putMetricRequestCapture
  }
}
