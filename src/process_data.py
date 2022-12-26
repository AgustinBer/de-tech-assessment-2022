import json
import pandas as pd
import glob

from datetime import timedelta


def process_data(**kwargs):
    # Initialize an empty list to store the processed data
    processed_data = []

    execution_date = kwargs["execution_date"]

    # Calculate the date for the previous day
    yesterday = execution_date - timedelta(days=1)

    # Format the date as a string in the format yyyy-mm-dd
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # Iterate over the files in the data directory
    for filename in glob.glob(f"data/{yesterday_str}*"):
        # Read the file and parse the JSON
        with open(f"data/{filename}") as f:
            contents = f.read()

        json_strings = contents.split("\n")
        json_strings = [x for x in json_strings if x != ""]

        # Iterate over the events in the file
        for json_string in json_strings:
            # Extract the relevant fields from the event
            event = json.loads(json_string)
            event_type = event["event"]
            entity = event["on"]
            timestamp = event["at"]
            organization_id = event["organization_id"]
            data = event["data"]

            # Check the type of event
            if entity == "vehicle":
                # Extract the vehicle data
                vehicle_id = data["id"]
                location = data["location"]
                lat = location["lat"]
                lng = location["lng"]
                location_timestamp = location["at"]

                # Add the data to the processed data list
                processed_data.append(
                    {
                        "event_type": event_type,
                        "entity": entity,
                        "timestamp": timestamp,
                        "organization_id": organization_id,
                        "vehicle_id": vehicle_id,
                        "lat": lat,
                        "lng": lng,
                        "location_timestamp": location_timestamp,
                    }
                )
            elif entity == "operating_period":
                # Extract the operating period data
                operating_period_id = data["id"]
                start = data["start"]
                finish = data["finish"]

                # Add the data to the processed data list
                processed_data.append(
                    {
                        "event_type": event_type,
                        "entity": entity,
                        "timestamp": timestamp,
                        "organization_id": organization_id,
                        "operating_period_id": operating_period_id,
                        "start": start,
                        "finish": finish,
                    }
                )

    # Convert the processed data to a Pandas DataFrame
    df = pd.DataFrame(processed_data)

    # Return the DataFrame
    return df
