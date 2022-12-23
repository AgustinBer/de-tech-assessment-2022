import json
import pandas as pd


def process_data(**kwargs):
    # Initialize an empty list to store the processed data
    processed_data = []

    # Iterate over the files in the data directory
    for filename in os.listdir("data"):
        # Read the file and parse the JSON
        with open(f"data/{filename}") as f:
            data = json.load(f)

        # Iterate over the events in the file
        for event in data:
            # Extract the relevant fields from the event
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
