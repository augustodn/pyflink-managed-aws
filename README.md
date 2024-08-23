# Streaming ETL with AWS Flink Managed Service

This repository contains the implementation of a streaming ETL pipeline using AWS Flink Managed Service. The project showcases a simple streaming filter.

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Local Installation](#local-installation)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

## Introduction

This project demonstrates how to set up a basic streaming ETL pipeline using Apache Flink on AWS. It covers filtering events in real-time, testing various windowing strategies, and optimizing performance. The focus is on IoT sensor data, but the principles can be applied to any streaming data use case.

## Prerequisites

Before you start, ensure you have the following installed:

- **Java 11** or later
- **Apache Maven** (for building the project)
- **Docker** (for local testing with Flink clusters)
- **AWS CLI** (for deployment to AWS)
- **Terraform** (if you're using infrastructure as code for AWS)

Additionally, you'll need an AWS account with permissions to deploy and manage Flink, S3, and other related services.

## Local Installation

Follow these steps to set up the project on your local machine:

1. **Clone the Repository**

   ```bash
   git clone https://github.com/yourusername/streaming-etl-flink.git
   cd streaming-etl-flink
   ```

2. **Build the Project**

   Make sure all dependencies are correctly installed by building the project using Maven:

   ```bash
   mvn clean install
   ```

3. **Set Up a Local Flink Cluster**

   You can use [poetry](https://python-poetry.org/docs/#installing-with-pipx) to set up a local Flink cluster:

   ```bash
   poetry install
   ```
   It’s strongly recommended to use poetry along with pyenv. So, in case you don’t have it installed yet, please do so by following this [guide](https://realpython.com/intro-to-pyenv/#installing-pyenv), just in case, this is the [official documentation](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation).


   This command will download the requirements needed to run Flink locally with all necessary components. It also matches the environment provided by AWS Managed Service.

4. **Configure AWS Credentials**

   Ensure your AWS CLI is configured with the appropriate credentials:

   ```bash
   aws configure
   ```

   You'll need access keys with permissions to manage Flink, S3, and related AWS services.

5. **Create the Kinesis Data Streams**

   Create the Kinesis Data Streams that will be used for the streaming data. You can also use an existing Kinesis Data Stream if you have it. In that case configure it properly in the `application_properties.json` file.

   ```bash
   aws kinesis create-stream --stream-name sensors --shard-count 1
   aws kinesis create-stream --stream-name alerts --shard-count 1
   ```

6. **Deploy Locally**

   1. Simulate the producer by generating the IoT packages locally:

   ```bash
   poetry run python iot_producer.py
   ```
   And run the Flink job locally to ensure everything is working:

   ```bash
   export IS_LOCAL="true"
   poetry run python iot_consumer.py
   ```
   You can also run the job from your preferred IDE.

## Deployment

Once you have tested the pipeline locally, you can deploy it to AWS using the following steps:

1. **Build and Package the Job**

   Ensure the Flink job is packaged correctly for deployment:

   ```bash
   mvn clean package
   ```

2. **Upload the JAR to S3**

   You'll need to upload the JAR file to an S3 bucket, which Flink will reference:

   ```bash
   aws s3 cp target/your-job.jar s3://your-bucket-name/
   ```

3. **Deploy to AWS Flink**

   Use the AWS Management Console or CLI to deploy your Flink job, referencing the S3 bucket and JAR file. You can follow further instructions in this [guide](link to blog).

4. **Monitor and Manage**

   Use the Flink Dashboard available through the AWS Console to monitor your job's performance and logs.

## Contributing

Contributions are welcome! If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are warmly welcome.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---
