# Project: Data Modeling with Cassandra

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring me on the project. My role was to create a database for this analysis. I was able to test my database by running queries given to me by the analytics team from Sparkify to create the results.

# Project Overview

In this project, I've applied what I've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python. To complete the project, I needed to model my data by creating tables in Apache Cassandra to run queries. Udacity provided part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

# Project Details

## Project Structure

+ [README.md](./README.md) This file.

+ [Project_1B.ipynb](./Project_1B.ipynb)  Jupyter Notebook file in which: a) we processed the `event_datafile_new.csv` dataset to create a denormalized dataset; b) I modeled the data tables keeping in mind the queries I needed to run; c) I loaded the data into tables I created in Apache Cassandra and ran my queries.

+ [event_datafile_new.csv](./event_datafile_new.csv) A smaller event data csv file created from the original data set files.

+ [images](./images) Directory containing image(s) used in this README and in the Jupyter Notebook.

+ [event_data](./event_data) Directory contaning event data csv files from November, 2018.

## Datasets

For this project, I worked with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## Sample data

The image below is a screenshot of what the denormalized data should appear like in the **event_datafile_new.csv**:

![Sample data](images/image_event_datafile_new.jpg)

