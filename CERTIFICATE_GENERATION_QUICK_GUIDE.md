# Certificate Generation Job - Quick Reference Guide

## What Does This Job Do?

The `collection-certificate-generator` generates certificates for course completions. When a user completes a course, this Flink job:
1. Receives a certificate request from Kafka
2. Validates the request
3. Checks if certificate was already issued
4. Generates the certificate (PDF/SVG with QR code)
5. Stores it in cloud storage
6. Updates database with certificate metadata
7. Sends notifications to the user

## Validations Performed

### 1. **Mandatory Fields Check**
- Certificate name (`edata.name`)
- SVG template URL (`edata.svgTemplate`)
- Recipient information (`edata.data.recipientName`)
- Issuer details (`edata.issuer.name`, `edata.issuer.url`)
- Signatory list (`edata.signatoryList` - name, id, designation, image)
- Criteria narrative (`edata.criteria.narrative`)

### 2. **Format Validations**
- Base path must be a valid URL
- Tag cannot contain special characters (except underscore)
- Public keys must be valid

### 3. **Duplicate Prevention**
- Queries Cassandra `user_enrolments` table
- Checks if certificate with same name already issued for this user/course/batch
- If already issued and no reissue flag, skips generation

## Certificate Generation Steps

### Traditional Mode (Old Registry):
1. Map request to CertModel
2. Generate certificate extension with recipient details
3. Create QR code for verification
4. Generate SVG certificate with embedded QR code
5. Upload certificate JSON to cloud storage
6. Add entry to certificate registry (HTTP API call)
7. Update `user_enrolments` table with certificate metadata
8. Send notification and create user feed entry

### RC Mode (Registry & Credentials - New):
1. Map request to CertModel
2. If reissuing, delete old certificate from RC
3. Extract learner profile from batch/course
4. Create Training object with course/batch details
5. Call RC Create API to generate verifiable credential
6. Update `user_enrolments` table
7. Send notification and create user feed entry

## Database Structure

### Table: `user_enrolments`
**Primary Key**: (userid, courseid, batchid)

**Key Columns**:
- `issued_certificates`: List of certificate metadata
  - `name`: Certificate name (e.g., "100PercentCompletionCertificate")
  - `identifier`: Unique certificate ID
  - `token`: Access/verification token
  - `lastIssuedOn`: Timestamp when issued
  - `templateUrl`: SVG template URL (for RC certificates)
  - `type`: Certificate type (for RC certificates)

## Supporting Features

1. **Learner Profile Extraction**: Determines learner profile from batch name or course metadata
2. **Notifications**: Sends SMS/email to notify users
3. **User Feed**: Creates in-app notification
4. **Audit Events**: Logs certificate issuance for tracking

## Key Data Flow

```
Kafka Topic: sunbirddev.generate.certificate.request
       ↓
Event Parsing (Event.scala)
       ↓
Validation (CertValidator.validateGenerateCertRequest)
       ↓
Duplicate Check (CertValidator.isNotIssued)
       ↓
Certificate Generation
  - Traditional: CertificateGeneratorFunction.generateCertificate
  - RC: CertificateGeneratorFunction.generateCertificateUsingRC
       ↓
Update Database (user_enrolments table)
       ↓
Output to Side Streams:
  - Audit events → Kafka
  - Notifications → NotifierFunction
  - User feed → CreateUserFeedFunction
```

## Required Changes for Activity Certificates

Activity certificates are similar to course certificates but don't have a batch concept.

### What Needs to Change:

1. **Event Model** - Add fields:
   - `activityId`: Identifier for the activity
   - `activityName`: Name of the activity

2. **Database Access** - Update queries to handle:
   - Courses: Use (userid, courseid, batchid)
   - Activities: Use (userid, activityid) - requires new table or schema change

3. **Training Type** - Modify Training object:
   - Support type="Course" (existing)
   - Support type="Activity" (new)
   - Make batchId optional (not needed for activities)

4. **Validation Logic** - Update duplicate check:
   - For courses: Check user_enrolments table
   - For activities: Check user_activity_enrolments table (or extended schema)

5. **Key Selector** - Update event partitioning:
   - For courses: Use userId + courseId + batchId
   - For activities: Use userId + activityId

### Sample Event for Activity Certificate:

```json
{
  "edata": {
    "userId": "user001",
    "activityId": "activity_workshop_123",
    "activityName": "Workshop on Data Science",
    "svgTemplate": "https://storage.com/template.svg",
    "templateId": "template_workshop_01",
    "name": "WorkshopCompletionCertificate",
    "data": [
      {
        "recipientName": "John Doe",
        "recipientId": "user001"
      }
    ],
    "issuer": {
      "name": "Training Institute",
      "url": "https://institute.org"
    },
    "signatoryList": [...],
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

### Database Options:

**Option 1**: Create new table `user_activity_enrolments`
- Primary Key: (userid, activityid)
- Same structure as user_enrolments
- Pros: Clean separation, no impact on existing code
- Cons: Code duplication for similar logic

**Option 2**: Extend existing table
- Add optional activityid column
- Use composite logic to query based on context
- Pros: Unified code
- Cons: Complex primary key handling, schema migration

**Recommendation**: Option 1 (separate table) for cleaner architecture

## Files to Modify:

1. `Event.scala` - Add activityId and activityName fields
2. `CertificateGeneratorConfig.scala` - Add activity-related constants
3. `CertValidator.scala` - Update isNotIssued() method
4. `CertificateGeneratorFunction.scala` - Update generateRequest() and updateUserEnrollmentTable()
5. `Models.scala` - Make batchId optional in Training case class
6. `CertificateGeneratorStreamTask.scala` - Update CertificateGeneratorKeySelector

## Testing Checklist:

- [ ] Test activity certificate generation (no courseId/batchId)
- [ ] Test course certificate generation (existing behavior)
- [ ] Test duplicate prevention for activities
- [ ] Test duplicate prevention for courses
- [ ] Test notifications for activities
- [ ] Test notifications for courses
- [ ] Test audit events for both types
- [ ] Test RC integration for activities
- [ ] Test RC integration for courses
- [ ] Test reissue for activities
- [ ] Test reissue for courses
