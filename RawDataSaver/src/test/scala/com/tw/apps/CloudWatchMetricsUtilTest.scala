package com.tw.apps

import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

class CloudWatchMetricsUtilTest extends AnyFeatureSpec with Matchers with MockFactory {

  val jobFlowFilePath = getClass.getResource("/job-flow.json").getPath

  Feature("Extract information from Job Flow file") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()

    Scenario("Parse job-flow id") {
      val listener = new CloudWatchMetricsUtil("some-app", jobFlowFilePath, mock[AmazonCloudWatch])

      listener.getJobFlowId should be("j-XN2TG35DXXN")
    }
  }
}
