package com.datapyro.emr.dynamo

import java.util
import java.security.MessageDigest

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{Row, SparkSession}
import com.datapyro.emr.common.EmrConstants
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.sql.functions.udf

/**
  * This example code reads NYSE data and writes to a table in DynamoDB
  * 
  * You should download NYSE data from https://s3.amazonaws.com/hw-sandbox/tutorial1/NYSE-2000-2001.tsv.gz and
  * create a table which a primary partition key called "id"
  *
  * Don't forget to set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.
  */
object SparkCSVToDynamoDB extends App {

  // check args
  if (args.length != 2) {
    println("Invalid usage! You should provide an input folder and a DynamoDB table name!")
    System.exit(-1)
  }
  val input = args(0)
  val dynamoTableName = args(1)

  // initialize context
  val sparkMaster: Option[String] = Option(System.getProperty("spark.master"))

  val spark = SparkSession.builder
    .master(sparkMaster.getOrElse("yarn"))
    .appName(getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  // add unique key column
  def getUniqueId() = udf((stockSymbol: String, date: String) => md5(stockSymbol + date))

  // load csv as a data frame
  val df = spark.read
    .option("sep", "\t")
    .option("header", "true")
    .csv(input)
    .withColumn("id", getUniqueId()($"stock_symbol", $"date"))

  df.printSchema()
  df.show()

  // write to dynamodb
  val region = EmrConstants.REGION
  val endpoint = s"dynamodb.$region.amazonaws.com"

  val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)
  jobConf.set("dynamodb.output.tableName", dynamoTableName)
  jobConf.set("dynamodb.endpoint", endpoint)
  jobConf.set("dynamodb.regionid", region)
  jobConf.set("dynamodb.servicename", "dynamodb")
  jobConf.set("dynamodb.throughput.write", "1")
  jobConf.set("dynamodb.throughput.write.percent", "1")
  jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
  jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")

  // save
  val columns = df.columns
  df.javaRDD
    .mapToPair(new PairFunction[Row, Text, DynamoDBItemWritable]() {
      @throws[Exception]
      override def call(row: Row): (Text, DynamoDBItemWritable) = {
        return (new Text(""), createItem(row, columns))
      }
    })
    .saveAsHadoopDataset(jobConf)

  // functions

  def createItem(row: Row, columns: Array[String]): DynamoDBItemWritable = {
    val attributes: util.Map[String, AttributeValue] = new util.HashMap[String, AttributeValue]
    // add fields as attributes
    for (column <- columns) {
      val value: Any = row.get(row.fieldIndex(column))
      if (value != null) {
        val attributeValue: AttributeValue = new AttributeValue
        if (value.isInstanceOf[String] || value.isInstanceOf[Boolean]) attributeValue.setS(value.toString)
        else attributeValue.setN(value.toString)
        attributes.put(column, attributeValue)
      }
    }
    // create row
    val item: DynamoDBItemWritable = new DynamoDBItemWritable
    item.setItem(attributes)
    item
  }

  def md5(text: String): String = MessageDigest.getInstance("MD5").digest(text.getBytes).map(0xFF & _)
    .map {"%02x".format(_) }.foldLeft("") { _ + _ }

}
