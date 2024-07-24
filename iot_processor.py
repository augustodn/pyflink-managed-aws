from pyflink.table import EnvironmentSettings, StreamTableEnvironment  # type: ignore
import os
import json

# 1. Creates a Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"  # on kda

is_local = (
    True if os.environ.get("IS_LOCAL") else False
)  # set this env var in your local environment

if is_local:
    # only for local, overwrite variable to properties and pass in your jars delimited by a semicolon (;)
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"  # local

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        f"file:///{CURRENT_DIR}/target/pyflink-dependencies.jar",
    )
    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.mode", "EXACTLY_ONCE"
    )

    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "1 min"
    )

def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print(f'A file at "{APPLICATION_PROPERTIES_FILE_PATH}" was not found')


def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


def create_input_table(table_name, stream_name, region, stream_initpos = None):
    init_pos = stream_initpos if stream_initpos else ''

    return f"""
        CREATE TABLE {table_name} (
            message_id VARCHAR(32),
            sensor_id INTEGER,
            message ROW(
                temperature FLOAT,
                pressure FLOAT,
                vibration FLOAT
            ),
            event_time TIMESTAMP_LTZ(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        )
        PARTITIONED BY (sensor_id)
        WITH (
            'connector' = 'kinesis',
            'stream' = '{stream_name}',
            'aws.region' = '{region}',
            'scan.stream.initpos' = '{init_pos}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """

def create_output_table(table_name, stream_name, region):
    return f"""
        CREATE TABLE {table_name} (
            message_id VARCHAR(32),
            sensor_id INTEGER,
            temperature FLOAT,
            alert STRING,
            event_time TIMESTAMP_LTZ(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        )
        PARTITIONED BY (sensor_id)
        WITH (
            'connector' = 'kinesis',
            'stream' = '{stream_name}',
            'aws.region' = '{region}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """

def create_s3_table(table_name, bucket_name):
    return f"""
        CREATE TABLE {table_name} (
            message_id VARCHAR(32),
            sensor_id INTEGER,
            message ROW(
                temperature FLOAT,
                pressure FLOAT,
                vibration FLOAT
            ),
            event_time TIMESTAMP_LTZ(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        )
        PARTITIONED BY (sensor_id)
        WITH (
            'connector' = 'filesystem',
            'path' = 's3://{bucket_name}/iot_raw_data',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'sink.partition-commit.policy.kind'='success-file',
            'sink.partition-commit.delay' = '1 min'
        )
    """

def create_print_table(table_name):
    return f"""
        CREATE TABLE {table_name} (
            message_id VARCHAR(32),
            sensor_id INTEGER,
            temperature FLOAT,
            alert STRING,
            event_time TIMESTAMP_LTZ(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        )
        PARTITIONED BY (sensor_id)
        WITH (
            'connector' = 'print'
        )
    """

def main():
    # Application Property Keys
    input_property_group_key = "consumer.config.0"
    producer_property_group_key = "producer.config.0"
    s3_sink_property_map_key = "producer.config.1"

    input_stream_key = "input.stream.name"
    input_region_key = "aws.region"
    input_starting_position_key = "scan.stream.initpos"

    output_stream_key = "output.stream.name"
    output_region_key = "aws.region"

    s3_bucket_key = "output.bucket"

    # tables
    input_table_name = "ExampleInputStream"
    output_table_name = "ExampleOutputStream"
    s3_table_name = "ExampleS3Table"

    # get application properties
    props = get_application_properties()
    print(props)

    input_property_map = property_map(props, input_property_group_key)
    output_property_map = property_map(props, producer_property_group_key)
    s3_sink_property_map = property_map(props, s3_sink_property_map_key)

    input_stream = input_property_map[input_stream_key]
    input_region = input_property_map[input_region_key]
    stream_initpos = input_property_map[input_starting_position_key]

    output_stream = output_property_map[output_stream_key]
    output_region = output_property_map[output_region_key]

    s3_bucket = s3_sink_property_map[s3_bucket_key]

    # 2. Creates a source table from a Kinesis Data Stream
    table_env.execute_sql(create_input_table(input_table_name, input_stream, input_region, stream_initpos))

    # 3. Creates a sink table writing to a Kinesis Data Stream
    table_env.execute_sql(create_output_table(output_table_name, output_stream, output_region))
    table_env.execute_sql(create_s3_table(s3_table_name, s3_bucket))

    # 4. Inserts the source table data into the sink table
    table_env.execute_sql(
        f"""
        INSERT INTO {output_table_name}
        SELECT
            message_id,
            sensor_id,
            message.temperature AS temperature,
            'High temperature detected' AS alert,
            event_time
        FROM {input_table_name}
        WHERE
            message.temperature > 30
        """
    )

    table_result = table_env.execute_sql(
        f"""
        INSERT INTO {s3_table_name}
        SELECT *
        FROM {input_table_name}
        """
    )

    # get job status through TableResult
    if is_local:
        table_result.wait()
    else:
        print(table_result.get_job_client().get_job_status())


if __name__ == "__main__":
    main()
