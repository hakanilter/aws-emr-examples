# AWS EMR Examples

This project contains several AWS EMR examples such as integrations between Spark, AWS S3, ElasticSearch, DynamoDB, etc.

- **SparkLogParser:**

    This simple Spark example parses a log file (e.g. CloudFront log) and executes a SQL query to do some aggregations.

- **SparkS3Aggregation:**

    This example reads a CSV file from S3 and creates a DataFrame in order to run some aggregation queries.

- **SparkCSVToElasticSearch:**

    Spark has very powerful integrations with other technologies, such as ElasticSearch. The example reads a CSV 
    file from S3 and saves into ElasticSearch with just a few lines of Spark code.        

- **SparkCSVTODynamoDB:**

    Like the previous one, this example shows how to save data into DynamoDB using Spark.

- **SparkDynamoDBQuery:** 

    Spark also can analysis the data on DynamoDB, this example shows how to do that.
         