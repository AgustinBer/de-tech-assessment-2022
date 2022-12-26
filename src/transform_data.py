import pandas as pd


def transform_data(**kwargs):
    # Extract the DataFrame from the task context
    df = kwargs["task_instance"].xcom_pull(task_ids="process_data")

    # Perform the data transformation
    df = df.rename(
        columns={
            "event_type": "event",
            "organization_id": "organization",
            "vehicle_id": "vehicle",
            "location_timestamp": "timestamp",
        }
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["start"] = pd.to_datetime(df["start"])
    df["finish"] = pd.to_datetime(df["finish"])
    df = df[
        [
            "event",
            "entity",
            "timestamp",
            "organization",
            "vehicle",
            "lat",
            "lng",
            "start",
            "finish",
        ]
    ]

    # Return the transformed DataFrame
    return df

transform_data()