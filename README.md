# Data Pipelines
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They are intended to create high-grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

## Dataset
The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to. 

So for this project, you'll be working with two datasets. Here are the s3 links for each:
- Log data: `s3://udacity-dend/log_data`
- Song data: `s3://udacity-dend/song_data`

## Project Instructions
To finish the project successfully, read the following instructions:
### Project Structure
The project structure package contains three major components for the project:
- The **dag template**: contains all the dag configuration and code.
- the **plugins template**, which includes:
  - The **operators** folder: contains all the defined operators needed to create the dag.
  - The **helpers** folder: contains a helper class for the SQL transformations.

### Configuring the DAG
In the DAG, add default parameters according to these guidelines:
- The DAG does not have dependencies on past runs.
- On failure, the task are retried 3 times.
- Retries happen every 5 minutes.
- Catchup is turned off.
- Do not email on retry.

In addition, configure the task dependencies so that after the dependencies are set, the graph view follows the flow shown in the image below.

![alt dag_graph](dag_graph.png)

### Building the operators
To complete the project, you need to build four different operators that will stage the data, transform the data, and run checks on data quality.

#### 1) Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The parameters should be used to distinguish between JSON file. 

Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

#### 2 & 3) Fact and Dimension Operators
Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

#### 4) Data Quality Operator
The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.


## Usage
To work on this project, you need first to install Python in your machine. Then, install the following dependencies:
- [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html)

### Prerequisites
- **Create an IAM User in AWS**. This to have aws credentials.
- **Create a redshift cluster in AWS**. Ensure that you are creating this cluster in the us-west-2 region. This is important as the s3-bucket that we are going to use for this project is in us-west-2.
- **Connect Airflow and AWS**
- **Connect Airflow to the AWS Redshift Cluster**














