package com.tw.apps

import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.{PutMetricDataRequest, StandardUnit}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.{times, verify}
import org.mockito.{ArgumentCaptor, Mockito}
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class SmokeTestTest extends FeatureSpec with MockitoSugar with Matchers with GivenWhenThen with BeforeAndAfter {
  val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
  val cloudWatchMock: AmazonCloudWatch = mock[AmazonCloudWatch]

  before {
    Mockito.reset(cloudWatchMock)
  }

  def readCsv(path : String) = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path)
  }

  def getProbes(df: sql.DataFrame) = {
    SmokeTest.runAssertions(df, 1594115095L)
  }

  feature("sending metrics to CloudWatch") {
    scenario("sends metrics for each probe") {
      val OVER_NINE_THOUSAND = 9001
      val THE_MEANING_OF_LIFE_THE_UNIVERSE_AND_EVERYTHING = 42
      val probes: List[Probe] = List(
        Probe("some-probe-metric", true, OVER_NINE_THOUSAND, StandardUnit.Count),
        Probe("another-probe-metric", false, THE_MEANING_OF_LIFE_THE_UNIVERSE_AND_EVERYTHING, StandardUnit.Count)
      )

      SmokeTest.publishMetrics(cloudWatchMock, probes, "")

      val requestCaptor : ArgumentCaptor[PutMetricDataRequest] = ArgumentCaptor.forClass(classOf[PutMetricDataRequest])
      verify(cloudWatchMock, times(2)).putMetricData(requestCaptor.capture())
    }

    scenario("sends metrics with the right arguments") {
      val OVER_NINE_THOUSAND = 9001
      val JOB_FLOW_ID = "mock-job-flow-id"
      val probes: List[Probe] = List(
        Probe("some-probe-metric", true, OVER_NINE_THOUSAND, StandardUnit.Count)
      )

      SmokeTest.publishMetrics(cloudWatchMock, probes, JOB_FLOW_ID)

      val requestCaptor : ArgumentCaptor[PutMetricDataRequest] = ArgumentCaptor.forClass(classOf[PutMetricDataRequest])
      verify(cloudWatchMock, times(1)).putMetricData(requestCaptor.capture())

      val request = requestCaptor.getValue
      request.getNamespace shouldEqual("stationMart-monitoring")
      val metricDatum = request.getMetricData.get(0)
      metricDatum.getValue shouldEqual(OVER_NINE_THOUSAND)
      metricDatum.getMetricName shouldEqual("some-probe-metric")
      metricDatum.getUnit shouldEqual("Count")
      metricDatum.getDimensions.get(0).getName shouldEqual("JobFlowId")
      metricDatum.getDimensions.get(0).getValue shouldEqual(JOB_FLOW_ID)
    }
  }

  feature("Recognizes various successes and failures") {
    scenario("passes and returns the latest `last_updated`") {

      val validDF = readCsv(getClass.getResource("/valid.csv").getPath)

      val probes = getProbes(validDF)

      val failureProbeCount = probes.count(_.failed)

      val ageProbe = probes.filter( _.name == "station-last-updated-age" )

      failureProbeCount shouldEqual(0)
      ageProbe(0).value shouldEqual(10)
    }

    scenario("recognises stale data") {
      val validDF = readCsv(getClass.getResource("/valid.csv").getPath)

      val probes = SmokeTest.runAssertions(validDF, 1594125095L)

      val failureProbeCount = probes.count( probe => probe.name == "station-last-updated-age" && probe.failed)

      failureProbeCount shouldEqual(1)
    }

    scenario("explodes when there are null longitudes") {
      val invalidDF = readCsv(getClass.getResource("/has-null-longitude.csv").getPath)
      val probes = getProbes(invalidDF)

      probes.count(_.failed) shouldEqual(1)
      probes.count( probe => probe.failed && probe.name == "null-longitude") shouldEqual(1)
    }

    scenario("explodes when there are null latitudes") {
      val invalidDF = readCsv(getClass.getResource("/has-null-latitude.csv").getPath)
      val probes = getProbes(invalidDF)

      probes.count(_.failed) shouldEqual(1)
      probes.count( probe => probe.failed && probe.name == "null-latitude") shouldEqual(1)
    }

    scenario("explodes when there are duplicated station_ids") {
      val invalidDF = readCsv(getClass.getResource("/has-duplicated-station-ids.csv").getPath)
      val probes = getProbes(invalidDF)

      probes.count(_.failed) shouldEqual(1)
      probes.count( probe => probe.failed && probe.name == "duplicated-station-ids") shouldEqual(1)
    }

    scenario("explodes when there are negative counts") {
      val invalidDF = readCsv(getClass.getResource("/has-negative-counts.csv").getPath)
      val probes = getProbes(invalidDF)

      probes.count(_.failed) shouldEqual(1)
      probes.count( probe => probe.failed && probe.name == "negative-counts") shouldEqual(1)
    }
  }
}
