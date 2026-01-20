# big-data-processing-apache-pig
Big data processing project using Apache Pig on HDFS, including data cleaning, joins, aggregation, and UDF development.

## Overview

This project demonstrates end-to-end big data processing using Apache Pig on Hadoop Distributed File System (HDFS). It covers data loading, cleaning, enrichment through joins, aggregation of metrics, and extension of Pig functionality using a custom Python User Defined Function (UDF).

## Technologies Used

* Apache Pig
* Hadoop Distributed File System (HDFS)
* Python (Pig UDF)
* Bash scripting
* AWS EMR (academic environment)

## Dataset

The project uses taxi trip datasets consisting of:

* Trips data (distance, fare, drop-off location)
* Taxi details (license, year, driver rating)
* Company information

The `data/` folder contains sample input files for reference.
During execution, these files are expected to be uploaded to HDFS under `/Input/` as required by the Pig scripts.

## Tasks Implemented

### 1. Data Loading and Cleaning

* Loaded multiple datasets using `PigStorage`
* Filtered invalid records and outliers
* Stored cleaned data back to HDFS

### 2. Data Enrichment

* Joined trips data with taxis and companies tables
* Generated enriched trip-level records with additional attributes

### 3. Aggregation and Analytics

* Computed company-level statistics:

  * Trip count
  * Total and average distance
  * Average fare
* Sorted results according to business requirements

### 4. Custom UDF Development

* Implemented a Python UDF to classify fares into LOW, MID, and HIGH bands
* Aggregated fare-band statistics per company

## Repository Structure

```
assignment/  - Assignment problem specification  
data/        - Sample input datasets  
pig/         - Apache Pig scripts  
udf/         - Python UDF implementation  
scripts/     - Execution scripts  
```

---

## How to Run

1. Upload input datasets to HDFS under `/Input/`
2. Execute the Pig script using:

   ```
   pig -x mapreduce a2.pig
   ```
3. Output results are written to HDFS under `/Output/`

(Execution environment depends on Hadoop cluster configuration.)

---

## Skills Demonstrated

* Big data ETL and analytics
* Distributed joins and aggregation
* Apache Pig scripting
* Python UDF development
* Understanding of HDFS-based workflows
* Technical documentation

---

## Author

Tanjeena Tahrin Islam (GitHub: https://github.com/TanjeenaT)  
Ama Jithmi Embuldeniya  
Shenaya Marise Tashiya Perera 

