package com.datapyro.emr.jdbc

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * This example shows how to import a CSV file into a table
  */
object SparkCSVToJDBC extends App {

  // check args
  if (args.length != 5) {
    println("Invalid usage! You should provide an input folder and jdbc url, user, password, table information")
    System.exit(-1)
  }
  val input = args(0)
  val url = args(1)
  val user = args(2)
  val pass = args(3)
  val table = args(4)

  // initialize context
  val sparkMaster: Option[String] = Option(System.getProperty("spark.master"))

  val spark = SparkSession.builder
    .master(sparkMaster.getOrElse("yarn"))
    .appName(getClass.getSimpleName)
    .getOrCreate()

  // load csv as a data frame
  val df = spark.read
    .option("sep", "\t")
    .option("header", "true")
    .csv(input)

  // write to database
  val properties: Properties = new Properties
  properties.setProperty("user", user)
  properties.setProperty("password", pass)

  df.write.jdbc(url, table, properties)

}
