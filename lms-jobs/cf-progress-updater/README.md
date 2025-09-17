# CF Progress Updater

A Flink streaming job that processes Competency Framework (CF) progress update events from Kafka and stores them in Cassandra.

## Overview

The CF Progress Updater is designed to handle Competency Framework progress update events, validate them, and store the progress information in a Cassandra database. It processes events from Kafka topics and ensures data consistency and reliability.

## Features

- **Event Processing**: Processes Competency Framework progress update events from Kafka
- **Data Validation**: Validates incoming events for required fields and data integrity
- **Database Storage**: Stores progress data in Cassandra with proper error handling
- **Metrics**: Comprehensive metrics collection for monitoring and observability
- **Scalability**: Supports parallel processing with configurable parallelism

## Event Structure

The job processes events with the following structure:

```json
{
  "actor": {"id": "CF Progress Processor", "type": "System"},
  "eid": "BE_JOB_REQUEST",
  "edata": {
    "userId": "user123",
    "activityId": "activity456",
    "activityType": "course",
    "batchId": "batch789",
    "action": "progress-update",
    "progress": 75.5,
    "identifier": "progress_001"
  },
  "partition": 0,
  "ets": "1.593769627322E12",
  "context": {
    "pdata": {"ver": 1.0, "id": "org.ekstep.platform"},
    "channel": "b00bc992ef25f1a9a8d63291e20efc8d",
    "env": "sunbirddev"
  },
  "mid": "LP.1593769627322.459a018c-5ec3-4c11-96c1-cd84d3786b85",
  "object": {"ver": "1593769626118", "id": "progress_001"}
}
```

## Supported Actions

- `progress-update`: Standard progress update events
- `cf-progress-update`: Competency Framework-specific progress update events

## Validation Rules

Events are considered valid if they meet the following criteria:

1. **Action**: Must be one of the supported actions (`progress-update` or `cf-progress-update`)
2. **Identifier**: Must not be blank
3. **User ID**: Must not be blank
4. **Activity ID**: Must not be blank
5. **Activity Type**: Must not be blank
6. **Progress**: Must be between 0.0 and 100.0 (inclusive)

## Database Schema

The job stores data in Cassandra with the following schema:

```sql
CREATE TABLE user_cf_progress (
    userId text,
    activityId text,
    activityType text,
    batchId text,
    progress double,
    updatedAt bigint,
    identifier text,
    PRIMARY KEY (userId, activityId, batchId)
);
```

## Configuration

### Kafka Configuration
- `kafka.input.topic`: Input Kafka topic for CF progress events
- `kafka.groupId`: Consumer group ID
- `task.consumer.parallelism`: Parallelism for Kafka consumer

### Cassandra Configuration
- `lms-cassandra.keyspace`: Cassandra keyspace
- `lms-cassandra.table`: Cassandra table name
- `lms-cassandra.host`: Cassandra host
- `lms-cassandra.port`: Cassandra port

### Task Configuration
- `task.parallelism`: Processing parallelism

## Metrics

The job provides the following metrics:

- `total-events-count`: Total number of events processed
- `success-events-count`: Number of successfully processed events
- `failed-events-count`: Number of failed events
- `skipped-event-count`: Number of skipped events (invalid)
- `db-write-count`: Number of database write operations

## Building and Running

### Prerequisites
- Java 11+
- Scala 2.12
- Maven 3.6+
- Apache Flink 1.16+
- Cassandra 3.11+
- Kafka 2.8+

### Build
```bash
mvn clean package
```

### Run
```bash
java -jar target/cf-progress-updater-1.0.0.jar --config.file.path /path/to/config.conf
```

## Testing

Run the test suite:
```bash
mvn test
```

The test suite includes:
- Unit tests for Event validation
- Integration tests for the complete processing pipeline
- Tests for error handling and edge cases
- Configuration validation tests

## Error Handling

The job handles various error scenarios:

1. **Invalid Events**: Events that don't meet validation criteria are skipped
2. **Database Errors**: Database connection failures are logged and metrics are updated
3. **Kafka Errors**: Kafka connection issues are handled by Flink's built-in retry mechanisms

## Monitoring

Monitor the job using the provided metrics:
- Check `success-events-count` vs `total-events-count` for processing success rate
- Monitor `failed-events-count` for database issues
- Watch `skipped-event-count` for data quality issues

## Contributing

1. Follow the existing code structure and patterns
2. Add comprehensive unit tests for new features
3. Update documentation for any configuration changes
4. Ensure all tests pass before submitting changes
