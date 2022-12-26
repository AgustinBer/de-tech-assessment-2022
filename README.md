# Data Warehouse for door2door

This repository contains a solution for building a simple and scalable data lake and data warehouse for door2door. The data lake and data warehouse enable the BI team to analyze data collected from the vehicles in door2door's fleet in real-time via GPS sensors.

## Folder structure

The repository has the following folder structure:

```
├── data
│ ├── raw
│ └── processed
├── dags
├── src
│ ├── fetch_data.py
│ ├── process_data.py
│ ├── transform_data.py
│ └── store_data.py
| └── main_dag.py
| └── main.py
├── Dockerfile
├── README.md
├── requirements.txt
└── tests.py
```


## Data fetching process

The data fetching process is responsible for getting data from the S3 bucket on a daily basis and storing it on the data lake. The data is stored in the `data/raw` folder in the repository. The process should only get data from a certain day on each run and should run every day.

The data fetching process is implemented in the `fetch_data.py` script.

## Data processing and extraction

The data processing and extraction process is responsible for processing and extracting the main events that occurred during operating periods. The process is implemented in the `process_data.py` script.

## Data transformation

The data transformation process is responsible for transforming the data into a format that is suitable for storing in the data warehouse. The process is implemented in the `transform_data.py` script.

## Data storing

The data storing process is responsible for storing the transformed data in a SQL-queriable format in the data warehouse. The process is implemented in the `store_data.py` script.

## Data governance and security

To ensure that the data is protected and compliant with relevant regulations, a data governance and security strategy has been implemented. The strategy includes data validation and cleansing steps in the data processing pipeline, as well as error handling and retry logic in the data fetching process.

## Running the solution locally

To run the solution locally, you need to have Docker installed. Then, follow these steps:

1. Build the Docker image: `docker build -t door2door .`
2. Run the Docker container: `docker run -p 80:80 door2door`

## Setting up the application on the cloud

To set up the application on the cloud, we recommend using [AWS](https://aws.amazon.com/). Here is a high-level plan for setting up the application on AWS:

1. Set up an AWS account.
2. Create an S3 bucket for storing the data.
3. Set up an EC2 instance for running the data processing and extraction scripts.
4. Set up an RDS instance for the data warehouse.
5. Set up an Airflow instance for orchestrating the scripts.
6. Set up a CloudWatch schedule for running the data fetching script daily.
7. Set up IAM roles for secure access to the S3 bucket and the RDS instance.

## Data modeling

To simplify the data, a data model has been created on the data warehouse layer. The data model consists of a single table called events, which contains the following columns:

event: the type of event on the entity
on: the entity on which the event occurred
at: the time at which the event occurred
data: an object containing information about the entity that the event occurred on
organization_id: the ID of the organization the event belongs to
The data column is a JSON object with the following structure:

{
    "id": "s0m3-v3h1cl3-1D",
    "location":
    {
        "lat": 50.1,
        "lng": 40.2,
        "at": "2016-02-02T01:00:00Z",  # ISO8601
    }
}

For vehicle events, the lat, lng, and at fields can be NULL if the vehicle is new and no location updates were received yet.

For operating period events, the data column has the following structure:

{
    "id": "s0m3-p3r1od-1D",
    "start": "2016-12-30T18:00:00Z",  # UTC time, ISO8601
    "finish": "2016-12-31T02:00:00Z"  # UTC time, ISO8601
}

The events table is denormalized to allow for efficient querying.


## Testing
Unit tests for the various scripts in the repository are contained in the tests.py file. To run the tests, use the following command:

```
python -m unittest tests/tests.py
```
