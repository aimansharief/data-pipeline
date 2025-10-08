# Course vs Activity Certificate Comparison

## Overview
This document provides a side-by-side comparison between course certificates and activity certificates to highlight the differences and similarities.

## Feature Comparison Table

| Feature | Course Certificate | Activity Certificate | Notes |
|---------|-------------------|---------------------|-------|
| **Primary Identifiers** | courseId, batchId | activityId | Activities don't have batches |
| **Name Field** | courseName | activityName | Same concept, different field |
| **Event Type** | `related.type = "course"` | `related.type = "activity"` | Type discrimination |
| **Database Table** | user_enrolments | user_activity_enrolments | Separate tables recommended |
| **Primary Key** | (userid, courseid, batchid) | (userid, activityid) | Different composite keys |
| **Training Type** | "Course" | "Activity" | Used in RC integration |
| **Batch Concept** | Required | Not applicable | Key difference |
| **Validation Logic** | Same | Same | No change needed |
| **Certificate Format** | Same | Same | SVG/PDF with QR code |
| **Notification Flow** | Same | Same | No change needed |
| **Duplicate Check** | Check user_enrolments | Check user_activity_enrolments | Different tables |

## Event Structure Comparison

### Course Certificate Event
```json
{
  "eid": "BE_JOB_REQUEST",
  "edata": {
    "userId": "user001",
    "courseName": "Introduction to Python",
    "templateId": "template_01",
    "svgTemplate": "https://storage.com/template.svg",
    "name": "100PercentCompletionCertificate",
    "data": [
      {
        "recipientName": "John Doe",
        "recipientId": "user001"
      }
    ],
    "issuer": {
      "name": "Training Institute",
      "url": "https://institute.org",
      "publicKey": ["key1"]
    },
    "signatoryList": [
      {
        "name": "Director",
        "id": "dir001",
        "designation": "Director",
        "image": "https://storage.com/signature.jpg"
      }
    ],
    "criteria": {
      "narrative": "Successfully completed course with 100% progress"
    },
    "related": {
      "type": "course",
      "courseId": "do_123456789",
      "batchId": "batch_0131000245281587206"
    }
  }
}
```

### Activity Certificate Event (Proposed)
```json
{
  "eid": "BE_JOB_REQUEST",
  "edata": {
    "userId": "user001",
    "activityName": "Workshop on Machine Learning",
    "templateId": "template_01",
    "svgTemplate": "https://storage.com/template.svg",
    "name": "WorkshopCompletionCertificate",
    "data": [
      {
        "recipientName": "John Doe",
        "recipientId": "user001"
      }
    ],
    "issuer": {
      "name": "Training Institute",
      "url": "https://institute.org",
      "publicKey": ["key1"]
    },
    "signatoryList": [
      {
        "name": "Director",
        "id": "dir001",
        "designation": "Director",
        "image": "https://storage.com/signature.jpg"
      }
    ],
    "criteria": {
      "narrative": "Successfully completed workshop"
    },
    "related": {
      "type": "activity",
      "activityId": "activity_workshop_123"
    }
  }
}
```

**Key Differences:**
1. `courseName` → `activityName`
2. `related.courseId` → `related.activityId`
3. `related.batchId` is absent in activity events
4. `related.type` is "activity" instead of "course"

## Database Schema Comparison

### user_enrolments (Course)
```sql
CREATE TABLE user_enrolments (
  userid text,
  courseid text,
  batchid text,
  coursename text,
  issued_certificates list<frozen<map<text, text>>>,
  completedOn timestamp,
  -- other columns...
  PRIMARY KEY (userid, courseid, batchid)
);
```

### user_activity_enrolments (Activity) - Proposed
```sql
CREATE TABLE user_activity_enrolments (
  userid text,
  activityid text,
  activityname text,
  issued_certificates list<frozen<map<text, text>>>,
  completedOn timestamp,
  -- other columns...
  PRIMARY KEY (userid, activityid)
);
```

**Key Differences:**
1. No `batchid` column in activity table
2. Primary key is simpler: (userid, activityid) vs (userid, courseid, batchid)
3. Column `coursename` → `activityname`
4. Same `issued_certificates` structure

## Code Changes Comparison

### 1. Event Model (Event.scala)

**Current (Course only):**
```scala
def courseId: String = related.getOrElse("courseId", "").asInstanceOf[String]
def batchId: String = related.getOrElse("batchId", "").asInstanceOf[String]
def courseName: String = readOrDefault[String]("edata.courseName", "")
```

**Modified (Course + Activity):**
```scala
def courseId: String = related.getOrElse("courseId", "").asInstanceOf[String]
def batchId: String = related.getOrElse("batchId", "").asInstanceOf[String]
def courseName: String = readOrDefault[String]("edata.courseName", "")

// New fields for activity
def activityId: String = related.getOrElse("activityId", "").asInstanceOf[String]
def activityName: String = readOrDefault[String]("edata.activityName", "")
```

### 2. Validation (CertValidator.scala)

**Current (Course only):**
```scala
def isNotIssued(event: Event)(config: CertificateGeneratorConfig, metrics: Metrics, cassandraUtil: CassandraUtil): Boolean = {
  val query = QueryBuilder.select("issued_certificates")
    .from(config.dbKeyspace, config.dbEnrollmentTable)
    .where(QueryBuilder.eq(config.dbUserId, event.userId))
    .and(QueryBuilder.eq(config.dbCourseId, event.courseId))
    .and(QueryBuilder.eq(config.dbBatchId, event.batchId))
  // ... rest of logic
}
```

**Modified (Course + Activity):**
```scala
def isNotIssued(event: Event)(config: CertificateGeneratorConfig, metrics: Metrics, cassandraUtil: CassandraUtil): Boolean = {
  val query = if (event.activityId.nonEmpty) {
    // Activity certificate
    QueryBuilder.select("issued_certificates")
      .from(config.dbKeyspace, config.dbActivityEnrollmentTable)
      .where(QueryBuilder.eq(config.dbUserId, event.userId))
      .and(QueryBuilder.eq(config.dbActivityId, event.activityId))
  } else {
    // Course certificate
    QueryBuilder.select("issued_certificates")
      .from(config.dbKeyspace, config.dbEnrollmentTable)
      .where(QueryBuilder.eq(config.dbUserId, event.userId))
      .and(QueryBuilder.eq(config.dbCourseId, event.courseId))
      .and(QueryBuilder.eq(config.dbBatchId, event.batchId))
  }
  // ... rest of logic
}
```

### 3. Training Model (Models.scala)

**Current (Course only):**
```scala
case class Training(
  id: String, 
  name: String, 
  `type`: String,  // Always "Course"
  batchId: String,  // Always required
  group: Option[String] = None, 
  completedOn: String
)
```

**Modified (Course + Activity):**
```scala
case class Training(
  id: String, 
  name: String, 
  `type`: String,  // Can be "Course" or "Activity"
  batchId: String = null,  // Optional, null for activities
  group: Option[String] = None, 
  completedOn: String
)
```

### 4. Certificate Generation (CertificateGeneratorFunction.scala)

**Current (Course only):**
```scala
def generateRequest(event: Event, certModel: CertModel, reIssue: Boolean): Map[String, AnyRef] = {
  val batchId = event.related.getOrElse(config.BATCH_ID, "").asInstanceOf[String]
  val courseId = event.related.getOrElse(config.COURSE_ID, "").asInstanceOf[String]
  
  val createCertReq = Map[String, AnyRef](
    "training" -> Training(courseId, event.courseName, "Course", batchId, /* ... */),
    // ... other fields
  )
  createCertReq
}
```

**Modified (Course + Activity):**
```scala
def generateRequest(event: Event, certModel: CertModel, reIssue: Boolean): Map[String, AnyRef] = {
  val (entityId, entityName, entityType, batchId) = if (event.activityId.nonEmpty) {
    (event.activityId, event.activityName, "Activity", null)
  } else {
    val bId = event.related.getOrElse(config.BATCH_ID, "").asInstanceOf[String]
    val cId = event.related.getOrElse(config.COURSE_ID, "").asInstanceOf[String]
    (cId, event.courseName, "Course", bId)
  }
  
  val createCertReq = Map[String, AnyRef](
    "training" -> Training(entityId, entityName, entityType, batchId, /* ... */),
    // ... other fields
  )
  createCertReq
}
```

### 5. Key Selector (CertificateGeneratorStreamTask.scala)

**Current (Course only):**
```scala
class CertificateGeneratorKeySelector extends KeySelector[Event, String] {
  override def getKey(event: Event): String = 
    Set(event.userId, event.courseId, event.batchId).mkString("_")
}
```

**Modified (Course + Activity):**
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

## Configuration Changes

### CertificateGeneratorConfig.scala

**Add these constants:**
```scala
// Activity-related constants
val dbActivityId = "activityid"
val dbActivityEnrollmentTable: String = config.getString("lms-cassandra.user_activity_enrolments.table")
val activity: String = "activity"
val activityName: String = "activityName"
```

### collection-certificate-generator.conf

**Add this configuration:**
```
lms-cassandra {
  keyspace = "sunbird_courses"
  user_enrolments.table = "user_enrolments"
  user_activity_enrolments.table = "user_activity_enrolments"  // New
  course_batch.table = "course_batch"
  sbkeyspace = "sunbird"
  certreg.table = "cert_registry"
}
```

## Testing Scenarios

### Course Certificate Tests (Existing)
1. ✓ Generate certificate for course completion
2. ✓ Prevent duplicate certificate generation
3. ✓ Handle reissue of certificates
4. ✓ Send notifications
5. ✓ Create audit events

### Activity Certificate Tests (New)
1. ✓ Generate certificate for activity completion
2. ✓ Prevent duplicate certificate generation for activities
3. ✓ Handle reissue of activity certificates
4. ✓ Send notifications for activity certificates
5. ✓ Create audit events for activity certificates

### Integration Tests
1. ✓ Course certificate doesn't affect activity certificates
2. ✓ Activity certificate doesn't affect course certificates
3. ✓ Both can coexist for same user
4. ✓ Key selector correctly routes events

## Migration Considerations

### If implementing activity certificates:

1. **Database Setup**:
   - Create `user_activity_enrolments` table
   - Set up proper indexes
   - Configure replication

2. **Configuration Updates**:
   - Update config files with new table name
   - Deploy configuration changes

3. **Code Deployment**:
   - Deploy updated code
   - Ensure backward compatibility with course certificates

4. **Testing**:
   - Test course certificates still work
   - Test activity certificates work
   - Test both types for same user

5. **Monitoring**:
   - Monitor both tables
   - Track success/failure rates
   - Monitor certificate generation time

## Summary

| Aspect | Complexity | Impact | Recommendation |
|--------|-----------|--------|----------------|
| Event Model | Low | Low | Add 2 new fields |
| Validation | Medium | Medium | Conditional logic |
| Database | High | High | New table needed |
| Generation | Medium | Medium | Conditional logic |
| Key Selector | Low | Low | Conditional logic |
| Models | Low | Low | Make field optional |
| **Overall** | **Medium** | **Medium** | **6-8 files to modify** |

The implementation is straightforward with conditional logic being the main pattern. The largest effort is database setup and testing to ensure backward compatibility.
