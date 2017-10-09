package com.datapyro.emr.elastic

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.datapyro.emr.common.Utils.md5
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * This example reads data from S3 and create an index on ElasticSearch
  */
object SparkCSVToElasticSearch extends App {

  // check args
  if (args.length != 2) {
    println("Invalid usage! You should provide an input folder and a elastic search host")
    System.exit(-1)
  }
  val input = args(0)
  val elasticHost = args(1)

  // initialize context
  val sparkMaster: Option[String] = Option(System.getProperty("spark.master"))

  val spark = SparkSession.builder
    .master(sparkMaster.getOrElse("yarn"))
    .appName(getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  // udf methods
  def getUniqueId() = udf((stockSymbol: String, date: String) => md5(stockSymbol + date))
  def processTime() = udf(() => DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()))
  val toDouble = udf[Double, String](_.toDouble)

  // load csv as a data frame
  val df = spark.read
    .option("sep", "\t")
    .option("header", "true")
    .csv(input)

  val extended = df
    // add required columns
    .withColumn("id", getUniqueId()($"stock_symbol", $"date"))
    .withColumn("process_time", processTime()())
    // change existing string fields to numeric fields
    .withColumn("stock_price_adj_close", toDouble(df("stock_price_adj_close")))
    .withColumn("stock_price_close", toDouble(df("stock_price_close")))
    .withColumn("stock_price_high", toDouble(df("stock_price_high")))
    .withColumn("stock_price_low", toDouble(df("stock_price_low")))
    .withColumn("stock_price_open", toDouble(df("stock_price_open")))
    .withColumn("stock_volume", toDouble(df("stock_volume")))

  extended.printSchema()

  // save to ES
  val options = Map(
    "pushdown" -> "true",
    "es.nodes.wan.only" -> "true",
    "es.nodes" -> elasticHost,
    "es.port" -> "9200",
    "es.mapping.id" -> "id"
  )

  EsSparkSQL.saveToEs(extended, "csv/item", options)

}
