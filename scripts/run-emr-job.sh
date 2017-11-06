#!/bin/bash

CLUSTER_ID="j-123456"
CLASS_NAME="com.datapyro.emr.spark.SparkS3Aggregation"
JAR_LOCATION="s3://datapyro-emr/example/app/emr-example-1.0.0-SNAPSHOT.jar"
INPUT_FOLDER="s3://datapyro-emr/example/data"
OUTPUT_FOLDER="s3://datapyro-emr/example/output"

aws emr add-steps --cluster-id $CLUSTER_ID --steps Type=spark,Name=EmrExample,Args=[--deploy-mode,cluster,--class,$CLASS_NAME,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,$JAR_LOCATION,$INPUT_FOLDER,$OUTPUT_FOLDER],ActionOnFailure=CONTINUE
