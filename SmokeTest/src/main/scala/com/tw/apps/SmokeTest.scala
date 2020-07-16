package com.tw.apps

import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
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

  def assertThatLatitudeAndLongitudeAreNeverNull(df: sql.DataFrame) = {
    var probes : List[(String, Boolean, Int, StandardUnit)] = List()

    val nullLongitudeCount = df.where("longitude is null").count().toInt
    probes = probes:+(("null-longitude", nullLongitudeCount > 0, nullLongitudeCount, StandardUnit.Count))

    val nullLatitudeCount = df.where("latitude is null").count().toInt
    probes = probes:+(("null-latitude", nullLatitudeCount > 0, nullLatitudeCount, StandardUnit.Count))

    probes
  }

  def assertThatNoCountsAreNegative(df: sql.DataFrame) = {
    var probes : List[(String, Boolean, Int, StandardUnit)] = List()
    val stationsWithNegativeCount = df.
        where("bikes_available < 0 OR docks_available < 0").count().toInt

    probes = (probes:+(("negative-counts", stationsWithNegativeCount > 0, stationsWithNegativeCount, StandardUnit.Count)))
    probes
  }

  def assertThatUpdatesAreRecent(df: DataFrame, currentTimeInSeconds: Long) = {
    var probes : List[(String, Boolean, Int, StandardUnit)] = List()

    val medianLastUpdateUnixTimestamp = df.stat.approxQuantile("last_updated", Array(0.5), 0.25)(0)
    val currentUnixTimestamp = currentTimeInSeconds

    val diffInSeconds = currentUnixTimestamp - medianLastUpdateUnixTimestamp

    probes = probes:+(("station-last-updated-age", diffInSeconds > 600, diffInSeconds.toInt, StandardUnit.Seconds))

    probes
  }

  def assertThatLatitudeAndLongitudeAreCorrectlyTyped(df: sql.DataFrame) = {
    var mistypedFields = 0
    var misnamedFields = 0
    var probes : List[(String, Boolean, Int, StandardUnit)] = List()

    if(df.schema.fields(7).name != "latitude") {
      misnamedFields = misnamedFields + 1
    }

    if(df.schema.fields(7).dataType.typeName != "double") {
      mistypedFields = mistypedFields + 1
    }

    if(df.schema.fields(8).name != "longitude") {
      misnamedFields = misnamedFields + 1
    }

    if(df.schema.fields(8).dataType.typeName != "double") {
      mistypedFields = mistypedFields + 1
    }

    probes = probes:+(("misnamed-fields", misnamedFields > 0, misnamedFields, StandardUnit.Count))
    probes = probes:+(("mistyped-fields", mistypedFields > 0, mistypedFields, StandardUnit.Count))

    //assert(df.schema.fields(7).name == "latitude", "field isn't called `latitude`")
    //assert(df.schema.fields(7).dataType.typeName == "double", "field `latitude` is not a double")
    //assert(df.schema.fields(8).name == "longitude", "field isn't called `longitude`")
    //assert(df.schema.fields(8).dataType.typeName == "double", "field `longitude` is not a double")

    probes
  }

  def assertThatStationIdsAreUnique(df: sql.DataFrame) = {
    var probes: List[(String, Boolean, Int, StandardUnit)] = List()

    val stationIdsWithMoreThanOneRow = df.select("station_id").groupBy("station_id").agg(count("station_id") as "count").where("count > 1").count().toInt
    probes = probes:+(("duplicated-station-ids", stationIdsWithMoreThanOneRow > 0, stationIdsWithMoreThanOneRow, StandardUnit.Count))
    probes
  }

  def runAssertions(df: DataFrame, cw: AmazonCloudWatch, currentTimeInSeconds: Long, jobFlowId: String) = {

    val probes = List.concat(
      assertThatStationIdsAreUnique(df),
      assertThatLatitudeAndLongitudeAreCorrectlyTyped(df),
      assertThatLatitudeAndLongitudeAreNeverNull(df),
      assertThatNoCountsAreNegative(df),
      assertThatUpdatesAreRecent(df, currentTimeInSeconds)
    )

    publishMetrics(cw, probes, jobFlowId)

    probes
  }

  def publishMetrics(cw: AmazonCloudWatch, probes: List[(String, Boolean, Int, StandardUnit)], jobFlowId: String) = {
    probes.map( probe => {
      publishMetric(cw, probe._1, probe._3, probe._4, jobFlowId)
    })
  }

  def publishMetric(cw: AmazonCloudWatch, metric : String, value: Int, unit: StandardUnit, jobFlowId: String): Unit = {

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

    val output = spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv(inputFile)
    val cw = AmazonCloudWatchClientBuilder.defaultClient
    val probes = runAssertions(output, cw, System.currentTimeMillis() / 1000, jobFlowId)

    val failures = probes.filter( probe => {
      probe._2 == true
    })

    val ageProbe = probes.filter( probe => {
      probe._1 == "station-last-updated-age"
    })

    println(ageProbe)

    if(failures.length > 0) {
      println(failures)
      System.exit(1)
    }
   }
}
