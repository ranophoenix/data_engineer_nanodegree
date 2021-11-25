# Project: Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring me into the project and expect me to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Overview

This project introduced me to the core concepts of Apache Airflow. To complete the project, I needed to create my own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

They have provided me with a project template that takes care of all the imports and provides four empty operators that needed to be implemented into functional pieces of a data pipeline. The template also contained a set of tasks that needed to be linked to achieve a coherent and sensible data flow within the pipeline.

I was provided with a helpers class that contains all the SQL transformations. Thus, I didn't need to write the ETL myself, but I needed to execute it with my custom operators.

## Datasets

For this project, I worked with two datasets. Here are the s3 links for each:

Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

## Configuring the DAG

In the DAG, I added default parameters according to these guidelines:

* The DAG does not have dependencies on past runs
* On failure, the task are retried 3 times
* Retries happen every 5 minutes
* Catchup is turned off
* Do not email on retry

In addition, I configured the task dependencies so that after the dependencies are set, the graph view follows the flow shown in the image below.

![DAG](./images/dag.png)

## Building the operators

To complete the project, I needed to build four different operators that staged the data, transformed the data, and ran checks on data quality.

All of the operators and task instances ran SQL statements against the Redshift database.

### Stage Operator

The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

### Fact and Dimension Operators

With dimension and fact operators, I used the provided SQL helper class to run data transformations. Most of the logic was within the SQL transformations and the operator was expected to take as input a SQL statement and target database on which to run the query against. 

### Data Quality Operator

The final operator to create was the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

## Project Structure

1. [airflow directory](./airflow) Folder containing the dag and the custom operators.
1. [images] Images used in this readme.
1. [README.md] This file.

## Requisites

These operators were tested using Apache Airflow 1.10.2.


