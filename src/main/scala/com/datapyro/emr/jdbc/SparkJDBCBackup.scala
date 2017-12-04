package com.datapyro.emr.jdbc

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * This example reads data from a jdbc source and saves as parquet
  *
  */
object SparkJDBCBackup extends App {

  // check args
  if (args.length != 5) {
    println("Invalid usage! You should provide jdbc url, user, password, table and an output folder")
    System.exit(-1)
  }
  val url = args(0)
  val user = args(1)
  val pass = args(2)
  val table = args(3)
  val output = args(4)

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
  df.show()

  // write as parquet
  df.write.parquet(output)

}
