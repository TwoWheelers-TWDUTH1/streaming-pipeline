package com.tw.apps

import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object SmokeTest {

  def assertThatLongitudeIsNeverNull(df: sql.DataFrame): Probe = {
    val nullLongitudeCount = df.where("longitude is null").count().toInt

    Probe("null-longitude", nullLongitudeCount > 0, nullLongitudeCount, StandardUnit.Count)
  }

  def assertThatLatitudeIsNeverNull(df: sql.DataFrame): Probe = {
    val nullLatitudeCount = df.where("latitude is null").count().toInt

    Probe("null-latitude", nullLatitudeCount > 0, nullLatitudeCount, StandardUnit.Count)
  }

  def assertThatNoCountsAreNegative(df: sql.DataFrame): Probe = {
    val stationsWithNegativeCount = df.where("bikes_available < 0 OR docks_available < 0").count().toInt

    Probe("negative-counts", stationsWithNegativeCount > 0, stationsWithNegativeCount, StandardUnit.Count)
  }

  def assertThatUpdatesAreRecent(df: DataFrame, currentTimeInSeconds: Long): Probe = {

    //TODO: this line causes OutOfBounds exceptions as the array can be empty
    val medianLastUpdateUnixTimestamp = df.stat.approxQuantile("last_updated", Array(0.5), 0.25)(0)
    val currentUnixTimestamp = currentTimeInSeconds

    val diffInSeconds = currentUnixTimestamp - medianLastUpdateUnixTimestamp

    Probe("station-last-updated-age", diffInSeconds > 600, diffInSeconds.toInt, StandardUnit.Seconds)
  }

  def assertThatLatitudeAndLongitudeAreCorrectlyTyped(df: sql.DataFrame): Probe = {
    var mistypedFields = 0

    if (df.schema.fields(7).dataType.typeName != "double") {
      mistypedFields = mistypedFields + 1
    }

    if (df.schema.fields(8).dataType.typeName != "double") {
      mistypedFields = mistypedFields + 1
    }

    Probe("mistyped-fields", mistypedFields > 0, mistypedFields, StandardUnit.Count)
  }

  def assertThatLatitudeAndLongitudeAreCorrectlyNamed(df: sql.DataFrame): Probe = {
    var misnamedFields = 0

    if (df.schema.fields(7).name != "latitude") {
      misnamedFields = misnamedFields + 1
    }

    if (df.schema.fields(8).name != "longitude") {
      misnamedFields = misnamedFields + 1
    }

    Probe("misnamed-fields", misnamedFields > 0, misnamedFields, StandardUnit.Count)
  }

  def assertThatStationIdsAreUnique(df: sql.DataFrame): Probe = {
    val stationIdsWithMoreThanOneRow = df.select("station_id").groupBy("station_id").agg(count("station_id") as "count").where("count > 1").count().toInt

    Probe("duplicated-station-ids", stationIdsWithMoreThanOneRow > 0, stationIdsWithMoreThanOneRow, StandardUnit.Count)
  }

  def runAssertions(df: DataFrame, currentTimeInSeconds: Long): List[Probe] = List(
    assertThatStationIdsAreUnique(df),
    assertThatLatitudeAndLongitudeAreCorrectlyTyped(df),
    assertThatLatitudeAndLongitudeAreCorrectlyNamed(df),
    assertThatLatitudeIsNeverNull(df),
    assertThatLongitudeIsNeverNull(df),
    assertThatNoCountsAreNegative(df),
    assertThatUpdatesAreRecent(df, currentTimeInSeconds)
  )

  def publishMetrics(cw: AmazonCloudWatch, probes: List[Probe], jobFlowId: String): Unit = {
    probes.foreach(probe => {
      publishMetric(cw, probe.name, probe.value, probe.unit, jobFlowId)
    })
  }

  def publishMetric(cw: AmazonCloudWatch, metric: String, value: Int, unit: StandardUnit, jobFlowId: String): Unit = {

    val dimensionJobFlowId = new Dimension()
      .withName("JobFlowId")
      .withValue(jobFlowId)

    val datum = new MetricDatum()
      .withMetricName(metric)
      .withUnit(unit)
      .withValue(value.toDouble)
      .withDimensions(dimensionJobFlowId)

    val request = new PutMetricDataRequest()
      .withNamespace("stationMart-monitoring")
      .withMetricData(datum)

    cw.putMetricData(request)
  }

  def parseJsonWithJackson(json: BufferedSource): mutable.Map[String, Object] = {
    val attrMapper = new ObjectMapper() with ScalaObjectMapper
    attrMapper.registerModule(DefaultScalaModule)
    attrMapper.readValue[mutable.Map[String, Object]](json.reader())
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      val message = "Argument required: <file>"
      throw new IllegalArgumentException(message)
    }

    val inputFile = args(0)

    val jobFlowInfoFile = "/mnt/var/lib/info/job-flow.json"
    val jobFlowId: String = parseJsonWithJackson(Source.fromFile(jobFlowInfoFile)).get("jobFlowId").mkString

    val spark = SparkSession.builder
      .appName("SmokeTest")
      .getOrCreate()

    val schema = ScalaReflection.schemaFor[StationData].dataType.asInstanceOf[StructType]

    val output = spark.read
      .schema(schema)
      .option("header", "true")
      .csv(inputFile)
      .persist(StorageLevel.DISK_ONLY)
    val cw = AmazonCloudWatchClientBuilder.defaultClient
    val probes = runAssertions(output, System.currentTimeMillis() / 1000)

    publishMetrics(cw, probes, jobFlowId)

    probes.filter(_.name == "station-last-updated-age").foreach(println)
    val failedProbes: List[Probe] = probes.filter(_.failed)

    if (failedProbes.nonEmpty)
      failedProbes.foreach(println)
    System.exit(1)
  }
}
