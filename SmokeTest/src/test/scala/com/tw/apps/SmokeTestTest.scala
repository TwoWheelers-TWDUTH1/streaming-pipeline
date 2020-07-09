package com.tw.apps

import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.{PutMetricDataRequest, StandardUnit}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.mockito.{ArgumentCaptor, Mockito}
import org.mockito.Mockito.verify
import org.mockito.Mockito.times
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
    SmokeTest.runAssertions(df, cloudWatchMock, 1594115095)
  }

  feature("sending metrics to CloudWatch") {
    scenario("sends metrics for each probe") {
      val OVER_NINE_THOUSAND = 9001
      val THE_MEANING_OF_LIFE_THE_UNIVERSE_AND_EVERYTHING = 42
      val probes: List[(String, Boolean, Int, StandardUnit)] = List(
        (
          "some-probe-metric", true, OVER_NINE_THOUSAND, StandardUnit.Count
        ),
        (
          "another-probe-metric", false, THE_MEANING_OF_LIFE_THE_UNIVERSE_AND_EVERYTHING, StandardUnit.Count
        )
      )

      SmokeTest.publishMetrics(cloudWatchMock, probes)

      val requestCaptor : ArgumentCaptor[PutMetricDataRequest] = ArgumentCaptor.forClass(classOf[PutMetricDataRequest])
      verify(cloudWatchMock, times(2)).putMetricData(requestCaptor.capture())
    }

    scenario("sends metrics with the right arguments") {
      val OVER_NINE_THOUSAND = 9001
      val probes: List[(String, Boolean, Int, StandardUnit)] = List(
        (
          "some-probe-metric", true, OVER_NINE_THOUSAND, StandardUnit.Count
        )
      )

      SmokeTest.publishMetrics(cloudWatchMock, probes)

      val requestCaptor : ArgumentCaptor[PutMetricDataRequest] = ArgumentCaptor.forClass(classOf[PutMetricDataRequest])
      verify(cloudWatchMock, times(1)).putMetricData(requestCaptor.capture())

      val request = requestCaptor.getValue
      println(request)

      val metricDatum = request.getMetricData.get(0)
      metricDatum.getValue shouldEqual(OVER_NINE_THOUSAND)
      metricDatum.getMetricName shouldEqual("some-probe-metric")
      metricDatum.getUnit shouldEqual("Count")
    }
  }

  feature("Recognizes various successes and failures") {
    scenario("passes and returns the latest `last_updated`") {
      val validDF = readCsv("./src/test/resources/valid.csv")

      val probes = getProbes(validDF)

      val failureProbeCount = probes.count( probe => {
        probe._2 == true
      })

      val ageProbe = probes.filter( probe => {
        probe._1 == "station-last-updated-age"
      })

      failureProbeCount shouldEqual(0)
      ageProbe(0)._3 shouldEqual(10)
    }

    scenario("recognises stale data") {
      val validDF = readCsv("./src/test/resources/valid.csv")

      val probes = SmokeTest.runAssertions(validDF, cloudWatchMock, 1594125095)

      val failureProbeCount = probes.count( probe => {
        probe._1 == "station-last-updated-age" && probe._2
      })

      failureProbeCount shouldEqual(1)
    }

    scenario("explodes when there are null longitudes") {
      val invalidDF = readCsv("./src/test/resources/has-null-longitude.csv")
      val probes = getProbes(invalidDF)

      probes.count(probe => { probe._2 }) shouldEqual(1)
      probes.count( probe => {
        probe._2 == true && probe._1 == "null-longitude"
      }) shouldEqual(1)
    }

    scenario("explodes when there are null latitudes") {
      val invalidDF = readCsv("./src/test/resources/has-null-latitude.csv")
      val probes = getProbes(invalidDF)

      probes.count(probe => { probe._2 }) shouldEqual(1)
      probes.count( probe => {
        probe._2 == true && probe._1 == "null-latitude"
      }) shouldEqual(1)
    }

    scenario("explodes when there are duplicated station_ids") {
      val invalidDF = readCsv("./src/test/resources/has-duplicated-station-ids.csv")
      val probes = getProbes(invalidDF)

      probes.count(probe => { probe._2 }) shouldEqual(1)
      probes.count( probe => {
        probe._2 == true && probe._1 == "duplicated-station-ids"
      }) shouldEqual(1)
    }

    scenario("explodes when there are negative counts") {
      val invalidDF = readCsv("./src/test/resources/has-negative-counts.csv")
      val probes = getProbes(invalidDF)

      probes.count(probe => { probe._2 }) shouldEqual(1)
      probes.count( probe => {
        probe._2 == true && probe._1 == "negative-counts"
      }) shouldEqual(1)
    }
  }
}