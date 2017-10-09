package com.datapyro.emr.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * This small example shows how to process log files such as CloudFront logs with Spark
  */
object SparkLogParser extends App {

  // check args
  if (args.length != 1) {
    println("Invalid usage! You should provide an input folder!")
    System.exit(-1)
  }
  val input = args(0)

  // initialize context
  val sparkMaster: Option[String] = Option(System.getProperty("spark.master"))

  val spark = SparkSession.builder
    .master(sparkMaster.getOrElse("yarn"))
    .appName(getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  // use a case class to map fields
  case class RawLog(
    dateTime: String,
    xEdgeLocation: String,
    scBytes: String,
    cIp: String,
    csMethod: String,
    csHost: String,
    csUriStem: String,
    scStatus: String,
    csReferer: String,
    csUserAgent: String,
    csUriQuery: String,
    csCookie: String,
    xEdgeResultType: String,
    xEdgeRequestId: String,
    xHostHeader: String,
    csProtocol: String,
    csBytes: String,
    timeTaken: String,
    xForwardedFor: String,
    sslProtocol: String,
    sslCipher: String,
    xEdgeResponseResultType: String,
    csProtocolVersion: String)

  // parse files and create a data set
  val ds = spark.sparkContext.textFile(input)
    .filter(line => !line.startsWith("#"))
    .map(line => line.split("\t"))
    .filter(_.length == 24)
    .map(a => RawLog(a(0) + a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8),
      a(9), a(10), a(11), a(12), a(13), a(14), a(15), a(16),
      a(17), a(18), a(19), a(20), a(21), a(22), a(23)))
    .toDF()
    .as[RawLog]

  // create a temp view
  ds.createOrReplaceTempView("logs")

  // do some aggregations
  ds.groupBy("cIp")
    .count()
    .orderBy(desc("count"))
    .limit(10)
    .show()
  
}
