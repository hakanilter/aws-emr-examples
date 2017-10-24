package com.datapyro.emr.ml

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
  * This example uses Kaggle "Credit Card Fraud Detection" Data set
  * in order to classify which record is fraud, which is not
  *
  * Test accuracy should be around 0.9987715562627232
  *
  * https://www.kaggle.com/dalpozz/creditcardfraud/data
  *
  */
object SparkCreditCardFraudClassifier extends App {

  // check args
  if (args.length != 1) {
    println("Invalid usage! You should provide an input folder for the data set!")
    System.exit(-1)
  }
  val input = args(0)

  // initialize context
  val sparkMaster: Option[String] = Option(System.getProperty("spark.master"))

  val spark = SparkSession.builder
    .master(sparkMaster.getOrElse("yarn"))
    .appName(getClass.getSimpleName)
    .getOrCreate()

  // load csv as a data frame
  val df = spark.read
    .option("sep", ",")
    .option("header", "true")
    .csv(input)

  df.printSchema()

  // convert to labeled point
  // the last column is the label, others are the features
  val data = df.rdd.map(row => {
    val label = row.getString(row.length - 1).toDouble
    val features = Vectors.dense(row.toSeq.toArray.slice(0, row.length-1).map({
                      case s: String => s.toDouble
                      case l: Long => l.toDouble
                      case _ => 0.0
                    }))
    LabeledPoint(label, features)
  })

  // Split data into training (70%) and test (30%).
  val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
  val training = splits(0).cache()
  val test = splits(1)

  // Run training algorithm to build the model
  val model = new LogisticRegressionWithLBFGS()
    .setNumClasses(2)
    .run(training)

  // Compute raw scores on the test set.
  val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
    val prediction = model.predict(features)
    (prediction, label)
  }

  // Get evaluation metrics.
  val metrics = new MulticlassMetrics(predictionAndLabels)
  val accuracy = metrics.accuracy
  println(s"Accuracy = $accuracy")

}
