import datetime
import json
import random
import boto3  # type: ignore
from typing import Any

STREAM_NAME = "ExampleInputStream"


def get_data() -> dict[str, Any]:
    return {
        'event_time': datetime.datetime.now().isoformat(),
        'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
        'price': round(random.random() * 100, 2)
    }


def generate(stream_name: str, kinesis_client: boto3.client) -> None:
    while True:
        data = get_data()
        print(data)
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey="partitionkey")


if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis', region_name='us-east-1'))
