# Certificate Generation Flow Diagrams

## Overall Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Kafka Topic                              │
│            sunbirddev.generate.certificate.request              │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│              CertificateGeneratorStreamTask                      │
│                                                                   │
│  ┌──────────────┐        ┌──────────────┐                       │
│  │ Kafka Source │──────▶ │ Event Parser │                       │
│  └──────────────┘        └──────┬───────┘                       │
│                                  │                                │
│                                  ▼                                │
│                    ┌─────────────────────────┐                   │
│                    │ CertificateGenerator     │                   │
│                    │ Function                 │                   │
│                    └─────────┬───────────────┘                   │
│                              │                                    │
│              ┌───────────────┼───────────────┐                   │
│              ▼               ▼               ▼                   │
│      ┌──────────────┐ ┌──────────┐ ┌──────────────┐            │
│      │ Audit Events │ │ Notifier │ │  User Feed   │            │
│      └──────┬───────┘ └────┬─────┘ └──────┬───────┘            │
│             │              │               │                     │
└─────────────┼──────────────┼───────────────┼─────────────────────┘
              ▼              ▼               ▼
         Kafka Topic    SMS/Email     User Feed DB
```

## Certificate Generation Process Flow

```
┌────────────────┐
│  Event Arrives │
└────────┬───────┘
         │
         ▼
┌────────────────────────────────┐
│ Parse Event (Event.scala)      │
│ Extract:                        │
│  - userId                       │
│  - courseId, batchId            │
│  - courseName                   │
│  - templateId, svgTemplate      │
│  - issuer, signatories          │
└────────┬───────────────────────┘
         │
         ▼
┌────────────────────────────────┐
│ Validate Request                │
│ (CertValidator.scala)           │
│                                 │
│ ✓ Mandatory fields present      │
│ ✓ Valid URL formats             │
│ ✓ Valid tag format              │
└────────┬───────────────────────┘
         │
         ▼
┌────────────────────────────────┐
│ Check Duplicate                 │
│ (isNotIssued)                   │
│                                 │
│ Query: user_enrolments table    │
│ WHERE userid = ?                │
│   AND courseid = ?              │
│   AND batchid = ?               │
│                                 │
│ Check: issued_certificates      │
└────────┬───────────────────────┘
         │
         ├─────────────────┐
         │                 │
    Already Issued?        │
         │                 │
      YES │              NO│
         │                 │
         ▼                 ▼
┌────────────────┐  ┌─────────────────────────┐
│ Skip & Log     │  │ Generate Certificate     │
│ (increment     │  │                          │
│ skipped count) │  │ Choose Mode:             │
└────────────────┘  │  - Traditional           │
                    │  - RC (Registry/Creds)   │
                    └──────────┬───────────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
              Traditional Mode        RC Mode
                    │                     │
                    ▼                     ▼
         ┌──────────────────┐  ┌──────────────────┐
         │ 1. Map to        │  │ 1. Map to        │
         │    CertModel     │  │    CertModel     │
         │                  │  │                  │
         │ 2. Generate      │  │ 2. Extract       │
         │    Certificate   │  │    learner       │
         │    Extension     │  │    profile       │
         │                  │  │                  │
         │ 3. Create QR     │  │ 3. Generate      │
         │    Code          │  │    Training obj  │
         │                  │  │                  │
         │ 4. Generate      │  │ 4. Call RC API   │
         │    SVG+QR        │  │    (POST)        │
         │                  │  │                  │
         │ 5. Upload JSON   │  │ 5. Get cert ID   │
         │    to Cloud      │  │                  │
         │                  │  │                  │
         │ 6. Add to Cert   │  │                  │
         │    Registry      │  │                  │
         └────────┬─────────┘  └─────────┬────────┘
                  │                      │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │ Update user_enrolments│
                  │ Table                 │
                  │                       │
                  │ SET issued_certificates│
                  │ WHERE userid = ?      │
                  │   AND courseid = ?    │
                  │   AND batchid = ?     │
                  └──────────┬────────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │ Generate Side Outputs│
                  │                       │
                  │ 1. Audit Event        │
                  │ 2. Notification       │
                  │ 3. User Feed          │
                  └───────────────────────┘
```

## Validation Flow

```
┌──────────────────────────────────────┐
│ validateGenerateCertRequest          │
└────────┬─────────────────────────────┘
         │
         ├─────────────────────┐
         │                     │
         ▼                     ▼
┌──────────────────┐  ┌──────────────────┐
│ Check edata      │  │ Check edata.data │
│  - name          │  │  - recipientName │
│  - svgTemplate   │  │                  │
└────────┬─────────┘  └────────┬─────────┘
         │                     │
         ▼                     ▼
┌──────────────────┐  ┌──────────────────┐
│ Check issuer     │  │ Check signatory  │
│  - name          │  │ List             │
│  - url           │  │  - name          │
│  - publicKey     │  │  - id            │
└────────┬─────────┘  │  - designation   │
         │            │  - image         │
         │            └────────┬─────────┘
         ▼                     │
┌──────────────────┐           │
│ Check criteria   │           │
│  - narrative     │           │
└────────┬─────────┘           │
         │                     │
         ├─────────────────────┘
         │
         ▼
┌──────────────────┐
│ Check tag format │
│ (no special      │
│  chars except _) │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Check basePath   │
│ (valid URL)      │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Check keys       │
│  - id            │
└──────────────────┘
```

## Changes Required for Activity Support

```
Current: Course Certificate
┌─────────────────────────┐
│ Event Structure         │
│  - userId               │
│  - courseId    ────┐    │
│  - batchId     ────┼──  Primary Key in DB
│  - courseName       │    │
└─────────────────────────┘

Database: user_enrolments
Primary Key: (userid, courseid, batchid)


New: Activity Certificate
┌─────────────────────────┐
│ Event Structure         │
│  - userId               │
│  - activityId   ────┐   │
│  - activityName     │   Primary Key in DB
│  (no batchId)       │   │
└─────────────────────────┘

Database: user_activity_enrolments
Primary Key: (userid, activityid)
```

## Modified Flow with Activity Support

```
┌────────────────┐
│  Event Arrives │
└────────┬───────┘
         │
         ▼
┌─────────────────────────────┐
│ Parse Event                  │
│                              │
│ Is activityId present?       │
└────┬────────────────┬────────┘
     │                │
     │ YES            │ NO
     │                │
     ▼                ▼
┌──────────────┐  ┌──────────────┐
│ Extract:     │  │ Extract:     │
│  - activityId│  │  - courseId  │
│  - activityName  │  - batchId   │
│              │  │  - courseName│
│ Type = Activity  │ Type = Course│
└──────┬───────┘  └──────┬───────┘
       │                 │
       └────────┬────────┘
                │
                ▼
       ┌────────────────┐
       │ Validation     │
       │ (same logic)   │
       └────────┬───────┘
                │
                ▼
       ┌────────────────────┐
       │ Duplicate Check     │
       │                     │
       │ If Activity:        │
       │   Query activity    │
       │   table            │
       │                     │
       │ If Course:          │
       │   Query enrolment   │
       │   table            │
       └────────┬───────────┘
                │
                ▼
       ┌────────────────────┐
       │ Generate Certificate│
       │                     │
       │ Training object:    │
       │  - id: activityId   │
       │    or courseId      │
       │  - type: Activity   │
       │    or Course        │
       │  - batchId: null    │
       │    or actual value  │
       └────────┬───────────┘
                │
                ▼
       ┌────────────────────┐
       │ Update Database     │
       │                     │
       │ If Activity:        │
       │   Update activity   │
       │   table            │
       │                     │
       │ If Course:          │
       │   Update enrolment  │
       │   table            │
       └─────────────────────┘
```

## Code Components to Modify

```
┌──────────────────────────────────────────────────────────┐
│                    Event.scala                            │
│  Add:                                                     │
│    def activityId: String = ...                          │
│    def activityName: String = ...                        │
└──────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│              CertificateGeneratorConfig.scala             │
│  Add:                                                     │
│    val dbActivityId = "activityid"                       │
│    val dbActivityEnrollmentTable = "user_activity_..."   │
└──────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│                  CertValidator.scala                      │
│  Modify:                                                  │
│    isNotIssued() - add conditional logic for activity    │
└──────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│          CertificateGeneratorFunction.scala               │
│  Modify:                                                  │
│    generateRequest() - handle activity type              │
│    updateUserEnrollmentTable() - query correct table     │
└──────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│                    Models.scala                           │
│  Modify:                                                  │
│    Training: make batchId optional (default null)        │
└──────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│        CertificateGeneratorStreamTask.scala               │
│  Modify:                                                  │
│    CertificateGeneratorKeySelector - handle activity     │
└──────────────────────────────────────────────────────────┘
```
