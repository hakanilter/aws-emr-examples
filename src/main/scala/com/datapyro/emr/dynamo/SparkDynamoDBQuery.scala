package com.datapyro.emr.dynamo

import com.datapyro.emr.common.EmrConstants
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  * This example shows how to query DynamoDB via Spark SQL
  */
object SparkDynamoDBQuery extends App {

  // check args
  if (args.length != 1) {
    println("Invalid usage! You should provide an input folder and a DynamoDB table name!")
    System.exit(-1)
  }
  val dynamoTableName = args(0)

  // initialize context
  val sparkMaster: Option[String] = Option(System.getProperty("spark.master"))

  val spark = SparkSession.builder
    .master(sparkMaster.getOrElse("yarn"))
    .appName(getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  // dynamodb config
  val region = EmrConstants.REGION
  val endpoint = s"dynamodb.$region.amazonaws.com"

  val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)
  jobConf.set("dynamodb.input.tableName", dynamoTableName)
  jobConf.set("dynamodb.endpoint", endpoint)
  jobConf.set("dynamodb.regionid", region)
  jobConf.set("dynamodb.servicename", "dynamodb")
  jobConf.set("dynamodb.throughput.read", "1")
  jobConf.set("dynamodb.throughput.read.percent", "1")
  jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
  jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")

  // use a case class for data set
  case class StockRecord(
    id: String,
    date: String,
    exchange: String,
    stock_price_adj_close: Double,
    stock_price_close: Double,
    stock_price_high: Double,
    stock_price_low: Double,
    stock_price_open: Double,
    stock_symbol: String,
    stock_volume: Double
  )

  // read data
  var nyse = spark.sparkContext
    .hadoopRDD(jobConf, classOf[DynamoDBInputFormat], classOf[Text], classOf[DynamoDBItemWritable])
    .map(_._2.getItem)
    .map(x => StockRecord(
          x.get("id").getS,
          x.get("date").getS,
          x.get("exchange").getS,
          x.get("stock_price_adj_close").getS.toDouble,
          x.get("stock_price_close").getS.toDouble,
          x.get("stock_price_high").getS.toDouble,
          x.get("stock_price_low").getS.toDouble,
          x.get("stock_price_open").getS.toDouble,
          x.get("stock_symbol").getS,
          x.get("stock_volume").getS.toDouble
    ))
    .toDF()
    .as[StockRecord]

  nyse.createOrReplaceTempView("NYSE")

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
  val result = spark.sql(sql)
  result.show()
  
}
