package com.datapyro.emr.elastic

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * This example shows how to run SQL query on ElasticSearch
  * an also shows how to create a new index from an another ElasticSearch index
  */
object SparkElasticSearchAnalysis extends App {

  // check args
  if (args.length != 1) {
    println("Invalid usage! You should provide an input folder and a elastic search host")
    System.exit(-1)
  }
  val elasticHost = args(0)

  // initialize context
  val sparkMaster: Option[String] = Option(System.getProperty("spark.master"))

  val spark = SparkSession.builder
    .master(sparkMaster.getOrElse("yarn"))
    .appName(getClass.getSimpleName)
    .getOrCreate()

  // read data from elastic search
  val options = Map(
    "pushdown" -> "true",
    "es.nodes.wan.only" -> "true",
    "es.nodes" -> elasticHost,
    "es.port" -> "9200",
    "es.mapping.id" -> "stock_symbol"
  )

  val df = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(options)
      .load("csv/item")

  df.registerTempTable("nyse")
  df.printSchema()

  // run query
  val sql =
    """
    SELECT
      stock_symbol,
      date,
      AVG(stock_price_open) AS avg_stock_price_open,
      SUM(stock_volume) AS total_stock_volume
    FROM nyse
    GROUP BY
      stock_symbol,
      date
    ORDER BY
      total_stock_volume DESC
  """

  // write to elastic search
  val result = spark.sql(sql)
      .repartition(20)
  EsSparkSQL.saveToEs(result, "nyse/stock", options)

}
