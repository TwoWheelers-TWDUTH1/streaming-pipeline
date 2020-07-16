package com.tw.apps

import org.scalamock.scalatest.MockFactory
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

class CloudWatchQuerySparkListenerTest extends AnyFeatureSpec with Matchers with MockFactory {

  Feature("Metrics are pushed to CloudWatch") {
    Scenario("onQueryStarted should send a is_app_running with value = 1") {
      val utilMock = mock[CloudWatchMetricsUtil]
      val listener = new CloudWatchQuerySparkListener("some-app", utilMock)

      (utilMock.pushCountMetric _).expects("is_app_running", 1).once

      listener.onQueryStarted(null)
    }

    ignore("onQueryProgress should send a is_app_running with value = 1") {
      val utilMock = mock[CloudWatchMetricsUtil]
      val listener = new CloudWatchQuerySparkListener("some-app", utilMock)

      (utilMock.pushCountMetric _).expects("is_app_running", 1).once

      listener.onQueryProgress(null)
    }

    ignore("onQueryTerminated should send a is_app_running with value = 0") {
      val utilMock = mock[CloudWatchMetricsUtil]
      val listener = new CloudWatchQuerySparkListener("some-app", utilMock)

      (utilMock.pushCountMetric _).expects("is_app_running", 0).once

      listener.onQueryTerminated(null)
    }
  }

  //    ignore("onQueryProgress should send a is_app_running with value = 1") {
  //      val cloudWatchClientMock = mock[AmazonCloudWatch]
  //      val listener = new CloudWatchQuerySparkListener("some-app", jobFlowFilePath, cloudWatchClientMock)
  //
  //      val putMetricRequestCapture = mockAndCapturePutMetricInteraction(cloudWatchClientMock, 200)
  //      listener.onQueryProgress(null)
  //
  //      putMetricRequestCapture.value.getNamespace should be("AWS/ElasticMapReduce")
  //      val metricData = putMetricRequestCapture.value.getMetricData.toList
  //      metricData.filter(d => "is_app_running".equals(d.getMetricName)).head.getValue should be(1.0d)
  //    }
  //
  //    Scenario("onQueryTerminated should send a is_app_running with value = 0") {
  //      val cloudWatchClientMock = mock[AmazonCloudWatch]
  //      val listener = new CloudWatchQuerySparkListener("some-app", jobFlowFilePath, cloudWatchClientMock)
  //
  //      val putMetricRequestCapture = mockAndCapturePutMetricInteraction(cloudWatchClientMock, 200)
  //      listener.onQueryTerminated(null)
  //
  //      putMetricRequestCapture.value.getNamespace should be("AWS/ElasticMapReduce")
  //      val metricData = putMetricRequestCapture.value.getMetricData.toList
  //      metricData.filter(d => "is_app_running".equals(d.getMetricName)).head.getValue should be(0.0d)
  //    }
  //  }
  //
  //  private def mockAndCapturePutMetricInteraction(utilMock: CloudWatchMetricsUtil, responseCode: Int): CaptureOne[PutMetricDataRequest] = {
  //    val sdkHttpMetadataMock = SdkHttpMetadata.from(
  //      new HttpResponse(null, null) {
  //        override def getStatusCode: Int = responseCode
  //      })
  //    val putMetricResultMock = mock[PutMetricDataResult]
  //    (putMetricResultMock.getSdkHttpMetadata _).expects().onCall(() => sdkHttpMetadataMock).atLeastOnce()
  //    val putMetricRequestCapture = CaptureOne[PutMetricDataRequest]()
  //    utilMock.pushCountMetric _ expects capture(putMetricRequestCapture) onCall { p: PutMetricDataRequest => putMetricResultMock } once
  //
  //    putMetricRequestCapture
  //  }
}
