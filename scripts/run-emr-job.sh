#!/bin/bash

CLUSTER_ID="j-854WGX1R6Z44"
CLASS_NAME="com.datapyro.emr.spark.SparkS3BinaryData"
JAR_LOCATION="s3://datapyro-main/lib/aws-emr-examples-1.0.0-SNAPSHOT-dist.jar"
INPUT_FOLDER="s3://datapyro-main/test"
OUTPUT_FOLDER="s3://datapyro-main/output"

aws emr add-steps --cluster-id $CLUSTER_ID --steps Type=spark,Name=EmrExample,Args=[--deploy-mode,cluster,--class,$CLASS_NAME,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,$JAR_LOCATION,$INPUT_FOLDER,$OUTPUT_FOLDER],ActionOnFailure=CONTINUE

# aws emr describe-step --cluster-id $CLUSTER_ID --step-id s-CK2QM0DYXS84