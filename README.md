

<br>

# Udacity Data Engineering Nanodegree | Data Pipeline Project

<br>

### *Arsalan Noorafkan*

**2021-07-06**
<br>
<br>

# **Overview**
## Background

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

##### ***These facts are for a fictional company that does not exist in real life***

<br>

## Purpose

This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

<br>

---

<br>

# **Datasets**

For this project, you'll be working with two datasets. Here are the s3 links for each:

- Log data: s3://udacity-dend/log_data
- Song data: s3://udacity-dend/song_data

<br>

# **Operators**

All of the operators and task instances will run SQL statements against the Redshift database. However, using parameters wisely will allow you to build flexible, reusable, and configurable operators you can later apply to many kinds of data pipelines with Redshift and with other databases.

## *Stage Operator*
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

## *Fact and Dimension Operators*
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

## *Data Quality Operator*
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.


## *Airflow DAG Graph View*
![Start Schema](assets/dag.png)



<br>
<br>
<br>


# **Instructions**
## Specifications
The ETL process comprises of the following technical specifications:
- Raw data is provided as JSON files stored in S3
    - Log data: s3://udacity-dend/log_data
    - Song data: s3://udacity-dend/song_data

    <br> 
    
- Python Libraries:
    - Apache Airflow

    <br> 
- AWS resources:
    - Redshift cluster


<br> 

## ETL Steps
The ETL process comprises of the following steps:

1) staging tables
    - events table
    - songs table
2) load fact table
    - songplays table
3) load dimension tables
    - user table
    - artist table
    - time table
    - song table
4) quality check
    - number of records
    - first record's columns

<br>


<br>
REMINDER: Do not include your AWS access keys in your code when sharing this project!
<br>

---


<br>

