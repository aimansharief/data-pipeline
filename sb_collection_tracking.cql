-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS sb_collection_tracking
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};


CREATE TABLE IF NOT EXISTS sb_collection_tracking.batches (
    activityid text,
    activitytype text,
    batchid text,
    cert_templates map<text, frozen<map<text, text>>>,
    created_date timestamp,
    createdby text,
    createdfor list<text>,
    description text,
    end_date timestamp,
    enrollment_enddate timestamp,
    enrollmenttype text,
    name text,
    start_date timestamp,
    status int,
    updated_date timestamp,
    PRIMARY KEY (activityid, batchid)
) WITH CLUSTERING ORDER BY (batchid ASC);

-- User enrolments table
CREATE TABLE IF NOT EXISTS sb_collection_tracking.user_enrolments (
    userid text,
    activityid text,
    activitytype text,
    batchid text,
    active boolean,
    addedby text,
    completedon timestamp,
    datetime timestamp,
    enrolled_date timestamp,
    issued_certificates list<frozen<map<text, text>>>,
    optional_batches list<text>,
    progress int,
    status int,
    statusmap map<text, int>,
    PRIMARY KEY (userid, activityid, activitytype, batchid)
) WITH CLUSTERING ORDER BY (activityid ASC, activitytype ASC, batchid ASC)

CREATE MATERIALIZED VIEW sb_collection_tracking.enrollments_by_batch AS
    SELECT activityid, activitytype, batchid, userid, active, addedby, completedon, datetime, enrolled_date, issued_certificates, optional_batches, progress, status, statusmap
    FROM sb_collection_tracking.user_enrolments
    WHERE activityid IS NOT NULL AND activitytype IS NOT NULL AND batchid IS NOT NULL AND userid IS NOT NULL
    PRIMARY KEY ((activityid, activitytype, batchid), userid)
    WITH CLUSTERING ORDER BY (userid ASC)
