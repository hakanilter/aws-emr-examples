package com.datapyro.emr.integration

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * This example reads data from a jdbc source and create an index on ElasticSearch
  *
  */
object SparkJDBCToElasticSearch extends App {

  // check args
  if (args.length != 6) {
    println("Invalid usage! You should provide jdbc url, user, password, table with ElasticSearch host name and index name!")
    System.exit(-1)
  }
  val url = args(0)
  val user = args(1)
  val pass = args(2)
  val table = args(3)
  val elasticHost = args(4)
  val indexName = args(5)

  // initialize context
  val sparkMaster: Option[String] = Option(System.getProperty("spark.master"))

  val spark = SparkSession.builder
    .master(sparkMaster.getOrElse("yarn"))
    .appName(getClass.getSimpleName)
    .getOrCreate()

  // read from database
  val properties: Properties = new Properties
  properties.setProperty("user", user)
  properties.setProperty("password", pass)

  val df = spark.read
      .jdbc(url, table, properties)   

  // write into ElasticSearch
  val options = Map(
    "pushdown" -> "true",
    "es.nodes.wan.only" -> "true",
    "es.nodes" -> elasticHost,
    "es.port" -> "9200"
  )

  EsSparkSQL.saveToEs(df, indexName, options)

}
