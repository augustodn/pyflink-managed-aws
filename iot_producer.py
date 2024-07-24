import concurrent.futures
import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3  # type: ignore

SLEEP_TIME = 5
REGION_NAME = "us-east-1"
STREAM_NAME = "ExampleInputStream"
MAX_WORKERS = 10


def generate_sensor_data() -> dict[str, Any]:
    """Generates random sensor data. It also adds a timestamp for traceability."""
    sensor_id = random.randint(1, 34)
    temperature = round(random.uniform(15, 35), 2)
    pressure = round(random.uniform(950, 1050), 2)
    vibration = round(random.uniform(0, 10), 2)
    sensor_data = {
        "message_id": uuid.uuid4().hex,
        "sensor_id": sensor_id,
        "message": {
            "temperature": temperature,
            "pressure": pressure,
            "vibration": vibration,
        },
        # utc timestamp
        "event_time": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }
    return sensor_data


def put_record(kinesis_cient: boto3.client, data: dict[str, Any]) -> None:
    kinesis_cient.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(data),
        PartitionKey="partitionkey",
    )
    print(f"Produced: {data}")
    time.sleep(SLEEP_TIME)


def main() -> None:
    """
    Controls the flow of the producer. It first subscribes to the topic and then
    generates sensor data and sends it to the topic.
    """
    kinesis_client = boto3.client("kinesis", region_name=REGION_NAME)
    while True:
        data_list = [generate_sensor_data() for _ in range(MAX_WORKERS)]
        # Send multiple records using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            records = [
                executor.submit(put_record, kinesis_client, sensor_data)
                for sensor_data in data_list
            ]
            concurrent.futures.wait(records)


if __name__ == "__main__":
    main()
