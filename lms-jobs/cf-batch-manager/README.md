# CF Batch Manager

This is a Flink job for managing CF (Content Framework) batches.

## Overview

The CF Batch Manager job processes batch-related events from Kafka and performs batch update operations.

## Configuration

The job is configured via `cf-batch-manager.conf` file which includes:

- Kafka input/output topics
- Task parallelism settings
- Cassandra database configuration
- Elasticsearch configuration
- Batch processing thresholds

## Main Components

- **CfBatchManagerStreamTask**: Main Flink streaming task
- **CfBatchManagerConfig**: Configuration class
- **BatchUpdaterFunction**: Core function for batch processing
- **Models**: Domain objects for batch data

## Building

```bash
mvn clean compile
```

## Running

```bash
java -cp target/cf-batch-manager-1.0.0.jar org.sunbird.job.cf.task.CfBatchManagerStreamTask
```

Or with config file:

```bash
java -cp target/cf-batch-manager-1.0.0.jar org.sunbird.job.cf.task.CfBatchManagerStreamTask --config.file.path /path/to/cf-batch-manager.conf
```
