import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def store_data(**kwargs):
    # Extract the DataFrame from the task context
    df = kwargs["task_instance"].xcom_pull(task_ids="transform_data")

    # Connect to the Redshift database
    conn = psycopg2.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=os.environ["REDSHIFT_PORT"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
        database=os.environ["REDSHIFT_DATABASE"],
    )
    cur = conn.cursor()

    # Create the table if it doesn't exist
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            event VARCHAR(255),
            entity VARCHAR(255),
            timestamp TIMESTAMP,
            organization VARCHAR(255),
            vehicle VARCHAR(255),
            lat NUMERIC,
            lng NUMERIC,
            start TIMESTAMP,
            finish TIMESTAMP
        );
    """
    )

    # Insert the data into the table
    for index, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO events (event, entity, timestamp, organization, vehicle, lat, lng, start, finish)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
            row,
        )
    conn.commit()

    # Close the connection
    cur.close()
    conn.close()
