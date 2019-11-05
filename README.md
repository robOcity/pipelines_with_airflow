# Data Pipelines with Airflow

Automate and monitor data pipelines using Apache Airflow that:

1. Extract and load (EL) data from AWS S3 JSON files into staging tables on AWS Redshift.

2. Extract, transform and load (ETL) the data into dimensional tables in Redshift.

3. Write the extract and load the dimensional tables back to S3.

## Configuration

Here I am using my project's repository to store all my Airflow code and configuration files.  This folder is known as AIRFLOW_HOME and it is stored in the environment.  I set the AIRFLOW_HOME by running ```export AIRFLOW_HOME=`pwd`/airflow```.

## References

1. [Can't import Airflow plugins](https://stackoverflow.com/questions/43907813/cant-import-airflow-plugins) - Stackoverflow post that discusses how to fix issues importing custom Airflow plugins.  
