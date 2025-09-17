CREATE KEYSPACE IF NOT EXISTS cf_progress WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE cf_progress;

CREATE TABLE IF NOT EXISTS user_cf_progress (
    userId text,
    activityId text,
    activityType text,
    batchId text,
    progress double,
    updatedAt bigint,
    identifier text,
    PRIMARY KEY (userId, activityId, batchId)
);
