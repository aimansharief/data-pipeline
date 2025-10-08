# Certificate Generation Job Analysis

## Overview
The `collection-certificate-generator` is a Flink streaming job that generates certificates for course completion. This document analyzes the current implementation and identifies changes needed to support certificate generation for "activity" similar to courses.

## Current Architecture

### 1. Main Components

#### A. Event Model (`Event.scala`)
- Parses incoming certificate generation requests from Kafka
- Key fields:
  - `userId`: User receiving the certificate
  - `courseId`: Course identifier (from `related.courseId`)
  - `batchId`: Batch identifier (from `related.batchId`)
  - `courseName`: Name of the course
  - `templateId`: Certificate template identifier
  - `svgTemplate`: SVG template URL for certificate design
  - `name`: Certificate name (e.g., "100PercentCompletionCertificate")
  - `data`: List of recipient data (recipientName, recipientId)
  - `issuer`: Issuer information
  - `signatoryList`: List of signatories
  - `criteria`: Certification criteria

#### B. Validation (`CertValidator.scala`)
The validator performs these checks:
1. **Mandatory Parameter Validation**:
   - `edata.name`: Certificate name
   - `edata.svgTemplate`: SVG template URL
   - `edata.data`: Recipient data with recipientName
   - `edata.issuer`: Issuer name and URL
   - `edata.signatoryList`: Signatory details (name, id, designation, image)
   - `edata.criteria`: Certificate criteria narrative

2. **Format Validation**:
   - Base path URL validation
   - Tag validation (no special characters except underscore)
   - Public key validation

3. **Duplicate Check** (`isNotIssued`):
   - Queries `user_enrolments` table in Cassandra
   - Checks `issued_certificates` column
   - Verifies if certificate with same name is already issued for user/course/batch combination

#### C. Certificate Generation (`CertificateGeneratorFunction.scala`)
The function has two modes:

**Mode 1: Traditional Certificate Generation** (`generateCertificate`):
1. Maps request to CertModel using CertMapper
2. Generates certificate extension
3. Creates QR code
4. Generates SVG with QR code embedded
5. Uploads JSON to cloud storage
6. Adds certificate to registry (via HTTP API)
7. Updates `user_enrolments` table in Cassandra

**Mode 2: RC (Registry & Credentials) Integration** (`generateCertificateUsingRC`):
1. Maps request to CertModel
2. If reissue, deletes old certificate from RC registry
3. Generates request with Training object containing:
   - `courseId`, `courseName`, `batchId`
   - Type: "Course"
   - Learner profile (extracted from batch name or course)
4. Calls RC Create API
5. Updates `user_enrolments` table

#### D. Database Schema (`user_enrolments` table)
Primary Keys:
- `userid`
- `courseid`
- `batchid`

Columns used:
- `issued_certificates`: List of issued certificates with metadata
  - `name`: Certificate name
  - `identifier`: Certificate ID
  - `token`: Access token
  - `lastIssuedOn`: Issue timestamp
  - `templateUrl`: Template URL (for RC certificates)
  - `type`: Certificate type (for RC certificates)

#### E. Supporting Features
- **Learner Profile Extraction**: 
  - Fetches course code and batch name
  - Extracts learner profile from batch naming convention
- **Notifications**: Sends SMS/email notifications to users
- **User Feed**: Creates user feed entries
- **Audit Events**: Generates audit trail

### 2. Data Flow

```
Kafka Topic (certificate request)
    ↓
Event Parsing
    ↓
Validation (CertValidator)
    ↓
Duplicate Check (isNotIssued)
    ↓
Certificate Generation
    ↓
Update user_enrolments table
    ↓
Send Notifications & Audit Events
```

## Changes Required for Activity Certificates

To support certificate generation for "activity" (similar to course), the following changes are needed:

### 1. Event Model Changes (`Event.scala`)
**Current**: Only supports `courseId`, `courseName`, `batchId`
**Required**: Add support for `activityId`, `activityName`

```scala
// Add new fields
def activityId: String = related.getOrElse("activityId", "").asInstanceOf[String]
def activityName: String = readOrDefault[String]("edata.activityName", "")
```

### 2. Configuration Changes (`CertificateGeneratorConfig.scala`)
**Required**: Add database field mappings for activity

```scala
val dbActivityId = "activityid"
val activity = "activity"
```

### 3. Database Schema Changes
**Current**: `user_enrolments` table uses (userid, courseid, batchid) as primary key
**Options**:
- **Option A**: Create new table `user_activity_enrolments` with (userid, activityid) as primary key
- **Option B**: Add optional `activityid` column to existing table (not recommended due to primary key constraint)
- **Option C**: Use a generic table structure to handle both courses and activities

### 4. Validation Changes (`CertValidator.scala`)
**Required**: Update `isNotIssued` method to check activity certificates

```scala
def isNotIssued(event: Event)(config: CertificateGeneratorConfig, metrics: Metrics, cassandraUtil: CassandraUtil): Boolean = {
  val query = if (event.activityId.nonEmpty) {
    // Query for activity certificates
    QueryBuilder.select("issued_certificates")
      .from(config.dbKeyspace, config.dbActivityEnrollmentTable)
      .where(QueryBuilder.eq(config.dbUserId, event.userId))
      .and(QueryBuilder.eq(config.dbActivityId, event.activityId))
  } else {
    // Existing course certificate query
    QueryBuilder.select("issued_certificates")
      .from(config.dbKeyspace, config.dbEnrollmentTable)
      .where(QueryBuilder.eq(config.dbUserId, event.userId))
      .and(QueryBuilder.eq(config.dbCourseId, event.courseId))
      .and(QueryBuilder.eq(config.dbBatchId, event.batchId))
  }
  // Rest of the logic remains same
}
```

### 5. Certificate Generation Changes (`CertificateGeneratorFunction.scala`)
**Required**: 
- Update `generateRequest` to handle activity type
- Modify Training object to support both Course and Activity types
- Update `updateUserEnrollmentTable` to write to appropriate table

```scala
def generateRequest(event: Event, certModel: CertModel, reIssue: Boolean): Map[String, AnyRef] = {
  val (entityId, entityName, entityType, batchId) = if (event.activityId.nonEmpty) {
    (event.activityId, event.activityName, "Activity", null)
  } else {
    (event.courseId, event.courseName, "Course", event.batchId)
  }
  
  // Update Training object
  val createCertReq = Map[String, AnyRef](
    "training" -> Training(entityId, entityName, entityType, batchId, learnerProfile, event.issuedDate)
    // ... rest of the fields
  )
}
```

### 6. Key Selector Changes (`CertificateGeneratorStreamTask.scala`)
**Required**: Update key generation to handle activities

```scala
class CertificateGeneratorKeySelector extends KeySelector[Event, String] {
  override def getKey(event: Event): String = {
    if (event.activityId.nonEmpty) {
      Set(event.userId, event.activityId).mkString("_")
    } else {
      Set(event.userId, event.courseId, event.batchId).mkString("_")
    }
  }
}
```

### 7. Models Changes (`Models.scala`)
**Required**: Update Training case class to make batchId optional

```scala
case class Training(
  id: String, 
  name: String, 
  `type`: String,  // Can be "Course" or "Activity"
  batchId: String = null,  // Make optional with default null
  group: Option[String] = None, 
  completedOn: String
)
```

## Summary of Changes

### Minimal Changes (Course and Activity share same structure):
1. Add `activityId` and `activityName` fields to Event model
2. Add conditional logic to use activity fields when `activityId` is present
3. Update Training type to support "Activity"
4. Make batchId optional in Training model
5. Update database queries to handle both course and activity contexts
6. Update key selector for proper event partitioning

### Database Considerations:
- If using separate table for activities, need to create `user_activity_enrolments` table
- If extending existing table, need schema migration to add `activityid` column
- Recommend creating a new table to maintain separation of concerns

### Testing Considerations:
- Test with activity events (no courseId/batchId, only activityId)
- Test with course events (existing behavior)
- Test duplicate prevention for activities
- Test notifications and audit events for activities

## Event Structure Example for Activity

```json
{
  "edata": {
    "userId": "user001",
    "activityName": "Workshop on Data Science",
    "activityId": "activity_123",
    "svgTemplate": "https://example.com/template.svg",
    "templateId": "template_01",
    "name": "WorkshopCompletionCertificate",
    "data": [{"recipientName": "John Doe", "recipientId": "user001"}],
    "issuer": {...},
    "signatoryList": [...],
    "criteria": {...},
    "related": {
      "type": "activity",
      "activityId": "activity_123"
    }
  }
}
```
