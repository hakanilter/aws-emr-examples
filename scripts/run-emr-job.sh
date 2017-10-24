#!/bin/bash
aws emr add-steps --cluster-id j-6VU4YCSB7H0B --steps Type=spark,Name=EmrExample,Args=[--deploy-mode,cluster,--class,com.datapyro.emr.spark.SparkEmrExample,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,s3://datapyro-emr/example/app/emr-example-1.0.0-SNAPSHOT.jar,s3://datapyro-emr/example/data,s3://datapyro-emr/example/output],ActionOnFailure=CONTINUE
