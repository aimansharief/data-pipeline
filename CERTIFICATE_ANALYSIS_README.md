# Certificate Generation Analysis - Documentation Index

## Purpose

This documentation analyzes the `collection-certificate-generator` Flink job to understand:
1. What validations and steps it performs to generate certificates
2. What changes are required to support certificate generation for "activity" (similar to course)

## Documentation Files

### 1. [CERTIFICATE_GENERATION_ANALYSIS.md](./CERTIFICATE_GENERATION_ANALYSIS.md)
**Comprehensive Technical Analysis**
- Detailed architecture overview
- Component-by-component breakdown
- Validation process explanation
- Database schema details
- Complete change requirements for activity support

**Best for**: Deep technical understanding, implementation planning

### 2. [CERTIFICATE_GENERATION_QUICK_GUIDE.md](./CERTIFICATE_GENERATION_QUICK_GUIDE.md)
**Quick Reference Guide**
- What the job does (summary)
- Validations performed (checklist)
- Generation steps (overview)
- Database structure (simplified)
- Changes required (summary)
- Event structure examples

**Best for**: Quick lookup, understanding the basics, stakeholder communication

### 3. [CERTIFICATE_GENERATION_FLOW_DIAGRAMS.md](./CERTIFICATE_GENERATION_FLOW_DIAGRAMS.md)
**Visual Flow Diagrams**
- Overall architecture diagram
- Certificate generation process flow
- Validation flow
- Modified flow with activity support
- Component modification map

**Best for**: Visual learners, understanding data flow, presentation to teams

### 4. [COURSE_VS_ACTIVITY_CERTIFICATE_COMPARISON.md](./COURSE_VS_ACTIVITY_CERTIFICATE_COMPARISON.md)
**Side-by-Side Comparison**
- Feature comparison table
- Event structure comparison
- Database schema comparison
- Code changes comparison (before/after)
- Testing scenarios
- Migration considerations

**Best for**: Implementation, understanding differences, development planning

## Quick Summary

### Current State
The certificate generator creates certificates for **course completions** with:
- **Primary Keys**: userId, courseId, batchId
- **Database**: user_enrolments table
- **Type**: "Course"

### Required for Activity Support
To generate certificates for **activities** (workshops, events, etc.):
- **Add Fields**: activityId, activityName
- **New Database**: user_activity_enrolments table with (userid, activityid) key
- **Support Type**: "Activity" (in addition to "Course")
- **Key Difference**: Activities don't have batches

### Files to Modify (6-8 files)
1. `Event.scala` - Add activityId, activityName fields
2. `CertificateGeneratorConfig.scala` - Add activity constants
3. `CertValidator.scala` - Update isNotIssued() for activity
4. `CertificateGeneratorFunction.scala` - Update generateRequest(), updateUserEnrollmentTable()
5. `Models.scala` - Make batchId optional in Training
6. `CertificateGeneratorStreamTask.scala` - Update key selector
7. Configuration files - Add activity table config
8. Database - Create user_activity_enrolments table

### Estimated Effort
- **Complexity**: Medium
- **Impact**: Medium (backward compatible)
- **Files Modified**: 6-8
- **Lines of Code**: ~150-200 LOC
- **Testing Required**: Course certs, activity certs, integration tests

## How to Use This Documentation

### For Stakeholders
1. Start with [CERTIFICATE_GENERATION_QUICK_GUIDE.md](./CERTIFICATE_GENERATION_QUICK_GUIDE.md)
2. Review [COURSE_VS_ACTIVITY_CERTIFICATE_COMPARISON.md](./COURSE_VS_ACTIVITY_CERTIFICATE_COMPARISON.md) - Feature comparison table

### For Developers
1. Read [CERTIFICATE_GENERATION_ANALYSIS.md](./CERTIFICATE_GENERATION_ANALYSIS.md) - Full technical details
2. Review [CERTIFICATE_GENERATION_FLOW_DIAGRAMS.md](./CERTIFICATE_GENERATION_FLOW_DIAGRAMS.md) - Understand data flow
3. Reference [COURSE_VS_ACTIVITY_CERTIFICATE_COMPARISON.md](./COURSE_VS_ACTIVITY_CERTIFICATE_COMPARISON.md) - Code changes section

### For Testers
1. Review [CERTIFICATE_GENERATION_QUICK_GUIDE.md](./CERTIFICATE_GENERATION_QUICK_GUIDE.md) - Testing checklist
2. Check [COURSE_VS_ACTIVITY_CERTIFICATE_COMPARISON.md](./COURSE_VS_ACTIVITY_CERTIFICATE_COMPARISON.md) - Testing scenarios

### For Architects
1. Review [CERTIFICATE_GENERATION_ANALYSIS.md](./CERTIFICATE_GENERATION_ANALYSIS.md) - Architecture section
2. Study [CERTIFICATE_GENERATION_FLOW_DIAGRAMS.md](./CERTIFICATE_GENERATION_FLOW_DIAGRAMS.md) - Component interactions
3. Check [COURSE_VS_ACTIVITY_CERTIFICATE_COMPARISON.md](./COURSE_VS_ACTIVITY_CERTIFICATE_COMPARISON.md) - Database considerations

## Current Job Capabilities

### ✅ Validates
- Mandatory fields (name, template, recipient, issuer, signatories, criteria)
- URL formats (base path)
- Tag format (no special characters)
- Public keys
- Duplicate certificates

### ✅ Generates
- Traditional certificates (SVG with QR code)
- RC certificates (Registry & Credentials integration)
- Certificate JSON metadata
- QR codes for verification

### ✅ Stores
- Certificate JSON in cloud storage
- Certificate metadata in Cassandra
- Optional: Certificate registry entries

### ✅ Notifies
- SMS notifications to users
- Email notifications to users
- User feed entries
- Audit events

### ✅ Supports
- Certificate reissue
- Multiple recipients per event
- Learner profile extraction
- Signatory management

## Recommended Implementation Approach

### Phase 1: Analysis & Design (Completed ✅)
- [x] Analyze current certificate generation
- [x] Document validations and steps
- [x] Identify required changes
- [x] Create documentation

### Phase 2: Database Setup
- [ ] Create user_activity_enrolments table
- [ ] Set up indexes
- [ ] Configure replication
- [ ] Test database access

### Phase 3: Code Changes
- [ ] Update Event model
- [ ] Update Config
- [ ] Update Validator
- [ ] Update Generator function
- [ ] Update Models
- [ ] Update Key selector
- [ ] Update tests

### Phase 4: Testing
- [ ] Unit tests for all changes
- [ ] Integration tests (course + activity)
- [ ] Regression tests (course still works)
- [ ] End-to-end tests

### Phase 5: Deployment
- [ ] Deploy database changes
- [ ] Deploy configuration
- [ ] Deploy code
- [ ] Monitor metrics

## Key Decisions Required

1. **Database Approach**
   - ✅ Recommended: Create separate `user_activity_enrolments` table
   - ❌ Not recommended: Extend existing table (complex primary key handling)

2. **Event Structure**
   - Use `related.activityId` and `edata.activityName`
   - Set `related.type = "activity"`

3. **Backward Compatibility**
   - Maintain full backward compatibility with course certificates
   - Use conditional logic to route to appropriate table/logic

## Support & Contact

For questions or clarifications on this analysis:
- Review the specific documentation file for your use case
- Check the comparison document for detailed examples
- Refer to flow diagrams for visual understanding

## Version History

- **v1.0** (Initial): Complete analysis with 4 documentation files
  - Full technical analysis
  - Quick reference guide
  - Visual flow diagrams
  - Course vs Activity comparison

---

**Location**: `/home/runner/work/data-pipeline/data-pipeline/`
**Job**: `lms-jobs/credential-generator/collection-certificate-generator/`
**Language**: Scala (Apache Flink)
**Database**: Cassandra (Sunbird)
