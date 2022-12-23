import unittest
import datetime
import boto3
import pandas as pd
import mysql.connector
import psycopg2


class TestDataLakeDataWarehouse(unittest.TestCase):
    def store_data(host, user, password, database, date, data, database_type):
        # connect to Redshift
        conn = psycopg2.connect(
            host=host, user=user, password=password, database=database
        )
        cursor = conn.cursor()
        # create the events table if it doesn't already exist
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS events (date DATE, event VARCHAR(255), on VARCHAR(255), at TIMESTAMP, data TEXT, organization_id VARCHAR(255))"""
        )
        # insert the data into the events table
        for event in data:
            cursor.execute(
                """INSERT INTO events (date, event, on, at, data, organization_id) VALUES (%s, %s, %s, %s, %s, %s)""",
                (
                    date,
                    event["event"],
                    event["on"],
                    event["at"],
                    str(event["data"]),
                    event["organization_id"],
                ),
            )
        conn.commit()
        cursor.close()
        conn.close()

    def setUp(self):
        # set up the test data and test environment
        self.s3_client = boto3.client("s3")
        self.s3_bucket = "test-bucket"
        self.s3_prefix = "test-prefix"
        self.mysql_host = "test-mysql-host"
        self.mysql_user = "test-mysql-user"
        self.mysql_password = "test-mysql-password"
        self.mysql_database = "test-mysql-database"
        self.redshift_host = "test-redshift-host"
        self.redshift_user = "test-redshift-user"
        self.redshift_password = "test-redshift-password"
        self.redshift_database = "test-redshift-database"
        self.date = datetime.datetime.now().strftime("%Y-%m-%d")
        self.data = [
            {
                "event": "create",
                "on": "vehicle",
                "at": "2019-05-19T16:02:02Z",
                "data": {
                    "id": "v1",
                    "location": {
                        "lat": 50.1,
                        "lng": 40.2,
                        "at": "2019-05-19T16:02:02Z",
                    },
                },
                "organization_id": "o1",
            },
            {
                "event": "update",
                "on": "vehicle",
                "at": "2019-05-19T17:03:03Z",
                "data": {
                    "id": "v1",
                    "location": {
                        "lat": 51.2,
                        "lng": 41.3,
                        "at": "2019-05-19T17:03:03Z",
                    },
                },
                "organization_id": "o1",
            },
            {
                "event": "deregister",
                "on": "vehicle",
                "at": "2019-05-19T18:04:04Z",
                "data": {
                    "id": "v1",
                    "location": {
                        "lat": 52.4,
                        "lng": 42.5,
                        "at": "2019-05-19T18:04:04Z",
                    },
                },
                "organization_id": "o1",
            },
        ]

    def test_store_data(self):
        # test the store_data function with MySQL
        store_data(
            self.mysql_host,
            self.mysql_user,
            self.mysql_password,
            self.mysql_database,
            self.date,
            self.data,
            "mysql",
        )
        # verify that the data has been stored in MySQL
        conn = mysql.connector.connect(
            host=self.mysql_host,
            user=self.mysql_user,
            password=self.mysql_password,
            database=self.mysql_database,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM events WHERE date = %s", (self.date,))
        rows = cursor.fetchall()
        self.assertEqual(len(rows), len(self.data))
        cursor.close()
        conn.close()

        # test the store_data function with Redshift
        store_data(
            self.redshift_host,
            self.redshift_user,
            self.redshift_password,
            self.redshift_database,
            self.date,
            self.data,
            "redshift",
        )
        # verify that the data has been stored in Redshift
        conn = psycopg2.connect(
            host=self.redshift_host,
            user=self.redshift_user,
            password=self.redshift_password,
            database=self.redshift_database,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM events WHERE date = %s", (self.date,))
        rows = cursor.fetchall()
        self.assertEqual(len(rows), len(self.data))
        cursor.close()
        conn.close()


if __name__ == "__main__":
    unittest.main()
