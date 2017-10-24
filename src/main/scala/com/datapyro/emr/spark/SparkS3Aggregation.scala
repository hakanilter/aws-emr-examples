package com.datapyro.emr.spark

import org.apache.spark.sql.SparkSession

/**
  * This example reads NYSE data from AWS S3, executes Spark SQL and saves the result on S3 as parquet.
  *
  * Download NYSE data from https://s3.amazonaws.com/hw-sandbox/tutorial1/NYSE-2000-2001.tsv.gz before running the job.
  *
  */
object SparkS3Aggregation extends App {

  // check args
  if (args.length != 2) {
    println("Invalid usage! You should provide input and output folders!")
    System.exit(-1)
  }
  val input = args(0)
  val output = args(1)

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
  df.createOrReplaceTempView("nyse")
  df.printSchema()

  // execute sql
  val sql = """
    SELECT
      stock_symbol,
      date,
      AVG(stock_price_open) AS avg_stock_price_open,
      SUM(stock_volume) AS total_stock_volume
    FROM nyse
    GROUP BY
      stock_symbol,
      date
  """
  val result = spark.sqlContext.sql(sql)

  // save results as parquet
  result.write
    .mode("overwrite")
    .parquet(output)

}
