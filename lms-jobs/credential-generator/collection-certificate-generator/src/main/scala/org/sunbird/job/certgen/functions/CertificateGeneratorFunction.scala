package org.sunbird.job.certgen.functions

import com.datastax.driver.core.querybuilder.{QueryBuilder, Update}
import com.datastax.driver.core.{Row, TypeTokens}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import kong.unirest.UnirestException
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.incredible.pojos.ob.CertificateExtension
import org.sunbird.incredible.processor.CertModel
import org.sunbird.incredible.processor.store.StorageService
import org.sunbird.incredible.processor.views.SvgGenerator
import org.sunbird.incredible.{CertificateConfig, CertificateGenerator, JsonKeys, ScalaModuleJsonUtils}
import org.sunbird.job.certgen.domain._
import org.sunbird.job.certgen.exceptions.ServerException
import org.sunbird.job.certgen.task.CertificateGeneratorConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.util.{CassandraUtil, ElasticSearchUtil, HttpUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

import java.io.{File, IOException}
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util
import java.util.stream.Collectors
import java.util.{Base64, Date}
import scala.collection.JavaConverters._

class CertificateGeneratorFunction(config: CertificateGeneratorConfig, httpUtil: HttpUtil, storageService: StorageService, @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessKeyedFunction[String, Event, String](config) {


  private[this] val logger = LoggerFactory.getLogger(classOf[CertificateGeneratorFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  val directory: String = "certificates/"
  val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  implicit val certificateConfig: CertificateConfig = CertificateConfig(basePath = config.basePath, encryptionServiceUrl = config.encServiceUrl, contextUrl = config.CONTEXT, issuerUrl = config.ISSUER_URL,
    evidenceUrl = config.EVIDENCE_URL, signatoryExtension = config.SIGNATORY_EXTENSION)
  implicit var esUtil: ElasticSearchUtil = null
  lazy private val gson = new Gson()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
    if(esUtil==null)
      esUtil = new ElasticSearchUtil(config.esConnection, config.certIndex, "config.auditHistoryIndexType")
  }

  override def close(): Unit = {
    cassandraUtil.close()
    if(esUtil!=null) esUtil.close()
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount, config.dbUpdateCount, config.enrollmentDbReadCount, config.totalEventsCount)
  }


  override def processElement(event: Event,
                              context: KeyedProcessFunction[String, Event, String]#Context,
                              metrics: Metrics): Unit = {
    println("Certificate data: " + event)
    metrics.incCounter(config.totalEventsCount)
    try {
      val certValidator = new CertValidator()
      val notIssued = certValidator.isNotIssued(event)(config, metrics, cassandraUtil)
      if(notIssued) {
        if(config.enableRcCertificate) {
          if (event.isActivity) generateActivityCertificateUsingRC(event, context)(metrics)
          else generateCourseCertificateUsingRC(event, context)(metrics)
        } else {
          if (event.isActivity) generateActivityCertificate(event, context)(metrics)
          else generateCourseCertificate(event, context)(metrics)
        }
      } else {
        val isActivity = event.isActivity
        val primaryFields = if (!isActivity) {
          Map(config.userId.toLowerCase() -> event.userId, config.batchId.toLowerCase -> event.batchId, config.courseId.toLowerCase -> event.courseId)
        } else {
          Map(config.userId.toLowerCase() -> event.userId,
            config.batchId.toLowerCase -> event.batchId,
            config.dbActivityId -> event.activityId,
            config.dbActivityType -> event.activityType)
        }
        metrics.incCounter(config.skippedEventCount)
      }
    } catch {
      case e: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(e.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), e)
    }
  }

  @throws[Exception]
  def generateCourseCertificate(event: Event, context: KeyedProcessFunction[String, Event, String]#Context)(implicit metrics: Metrics): Unit = {
    logger.info(s"generateCourseCertificate: Generating certificate for userId=${event.eData.getOrElse("userId", "")}, courseId=${event.related.getOrElse(config.COURSE_ID, "")}, batchId=${event.related.getOrElse(config.BATCH_ID, "")}")
    val certModelList: List[CertModel] = new CertMapper(certificateConfig).mapReqToCertModel(event)
    val certificateGenerator = new CertificateGenerator
    certModelList.foreach(certModel => {
      var uuid: String = null
      try {
        logger.info(s"generateCourseCertificate: Generating CertModel for userId=${certModel.identifier}, recipientName=${certModel.recipientName}, certificateName=${certModel.certificateName}")
        val certificateExtension: CertificateExtension = certificateGenerator.getCertificateExtension(certModel)
        uuid = certificateGenerator.getUUID(certificateExtension)
        logger.info(s"generateCourseCertificate: Generated UUID=$uuid for userId=${certModel.identifier}")
        val qrMap = certificateGenerator.generateQrCode(uuid, directory, certificateConfig.basePath)
        val encodedQrCode: String = encodeQrCode(qrMap.qrFile)
        val printUri = SvgGenerator.generate(certificateExtension, encodedQrCode, event.svgTemplate)
        certificateExtension.printUri = Option(printUri)
        val jsonUrl = uploadJson(certificateExtension, directory.concat(uuid).concat(".json"), event.tag.concat("/"))
        val addReq = Map[String, AnyRef](JsonKeys.REQUEST -> {Map[String, AnyRef](
          JsonKeys.ID -> uuid, JsonKeys.JSON_URL -> certificateConfig.basePath.concat(jsonUrl),
          JsonKeys.JSON_DATA -> certificateExtension, JsonKeys.ACCESS_CODE -> qrMap.accessCode,
          JsonKeys.RECIPIENT_NAME -> certModel.recipientName, JsonKeys.RECIPIENT_ID -> certModel.identifier,
          config.RELATED -> event.related
        ) ++ {if (event.oldId.nonEmpty) Map[String, AnyRef](config.OLD_ID -> event.oldId) else Map[String, AnyRef]()}})
        logger.info(s"generateCourseCertificate: Adding certificate to registry for userId=${certModel.identifier}, uuid=$uuid")
        addCertToRegistry(event, addReq, context)(metrics)
        val related = event.related
        val userEnrollmentData = UserEnrollmentData(related.getOrElse(config.BATCH_ID, "").asInstanceOf[String], certModel.identifier,
          related.getOrElse(config.COURSE_ID, "").asInstanceOf[String], event.courseName, event.templateId,
          Certificate(uuid, event.name, qrMap.accessCode, formatter.format(new Date()), "", ""))
        logger.info(s"generateCourseCertificate: Updating user enrollment table for userId=${certModel.identifier}, batchId=${related.getOrElse(config.BATCH_ID, "")}, courseId=${related.getOrElse(config.COURSE_ID, "")}")
        updateUserEnrollmentTable(event, userEnrollmentData, context)
        metrics.incCounter(config.successEventCount)
        logger.info(s"generateCourseCertificate: Certificate generation completed for userId=${certModel.identifier}, uuid=$uuid")
      } finally {
        cleanUp(uuid, directory)
      }
    })
  }

  @throws[Exception]
  def generateCourseCertificateUsingRC(event: Event, context: KeyedProcessFunction[String, Event, String]#Context)(implicit metrics: Metrics): Unit = {
    logger.info(s"generateCourseCertificateUsingRC: Generating certificate for userId=${event.eData.getOrElse("userId", "")}, courseId=${event.related.getOrElse(config.COURSE_ID, "")}, batchId=${event.related.getOrElse(config.BATCH_ID, "")}")
    val certModelList: List[CertModel] = new CertMapper(certificateConfig).mapReqToCertModel(event)
    certModelList.foreach(certModel => {
      var uuid: String = null
      val reIssue: Boolean = event.oldId.nonEmpty
      if(reIssue){
        try { callCertificateRc(config.rcDeleteApi, event.oldId, null) } catch {
          case ex: ServerException =>
            logger.error("Rc deletion failed | old id is not present :: identifier " + event.oldId + " :: " + ex.getMessage)
            deleteOldRegistry(event.oldId)
          case e: UnirestException => logger.error("Rc deletion failed due to connection :: identifier " + event.oldId + " :: " + e.getMessage)
        }
      }
      logger.info(s"generateCourseCertificateUsingRC: Generating CertModel for userId=${certModel.identifier}, recipientName=${certModel.recipientName}, certificateName=${certModel.certificateName}")
      val related = event.related
      val certReq = generateRequest(event, certModel, reIssue)
      uuid = callCertificateRc(config.rcCreateApi, null, certReq)
      logger.info(s"generateCourseCertificateUsingRC: Certificate RC created for userId=${certModel.identifier}, uuid=$uuid")
      val userEnrollmentData = UserEnrollmentData(related.getOrElse(config.BATCH_ID, "").asInstanceOf[String], certModel.identifier,
        related.getOrElse(config.COURSE_ID, "").asInstanceOf[String], event.courseName, event.templateId,
        Certificate(uuid, event.name, "", formatter.format(new Date()), event.svgTemplate, config.rcEntity))
      logger.info(s"generateCourseCertificateUsingRC: Updating user enrollment table for userId=${certModel.identifier}, batchId=${related.getOrElse(config.BATCH_ID, "")}, courseId=${related.getOrElse(config.COURSE_ID, "")}")
      updateUserEnrollmentTable(event, userEnrollmentData, context)
      metrics.incCounter(config.successEventCount)
      logger.info(s"generateCourseCertificateUsingRC: Certificate generation completed for userId=${certModel.identifier}, uuid=$uuid")
    })
  }

  @throws[Exception]
  def generateActivityCertificate(event: Event, context: KeyedProcessFunction[String, Event, String]#Context)(implicit metrics: Metrics): Unit = {
    logger.info(s"generateActivityCertificate: Generating certificate for userId=${event.eData.getOrElse("userId", "")}, activityType=${event.eData.getOrElse("activityType", "")}, activityName=${event.activityName}, batchId=${event.eData.getOrElse("batchId", "")}")
    val certModelList: List[CertModel] = new CertMapper(certificateConfig).mapReqToCertModel(event)
    val certificateGenerator = new CertificateGenerator
    certModelList.foreach(certModel => {
      var uuid: String = null
      try {
        logger.info(s"generateActivityCertificate: Generating CertModel for userId=${certModel.identifier}, recipientName=${certModel.recipientName}, certificateName=${certModel.certificateName}")
        val certificateExtension: CertificateExtension = certificateGenerator.getCertificateExtension(certModel)
        uuid = certificateGenerator.getUUID(certificateExtension)
        logger.info(s"generateActivityCertificate: Generated UUID=$uuid for userId=${certModel.identifier}")
        val qrMap = certificateGenerator.generateQrCode(uuid, directory, certificateConfig.basePath)
        val encodedQrCode: String = encodeQrCode(qrMap.qrFile)
        val printUri = SvgGenerator.generate(certificateExtension, encodedQrCode, event.svgTemplate)
        certificateExtension.printUri = Option(printUri)
        val jsonUrl = uploadJson(certificateExtension, directory.concat(uuid).concat(".json"), event.tag.concat("/"))
        val addReq = Map[String, AnyRef](JsonKeys.REQUEST -> {Map[String, AnyRef](
          JsonKeys.ID -> uuid, JsonKeys.JSON_URL -> certificateConfig.basePath.concat(jsonUrl),
          JsonKeys.JSON_DATA -> certificateExtension, JsonKeys.ACCESS_CODE -> qrMap.accessCode,
          JsonKeys.RECIPIENT_NAME -> certModel.recipientName, JsonKeys.RECIPIENT_ID -> certModel.identifier,
          config.RELATED -> event.related
        ) ++ {if (event.oldId.nonEmpty) Map[String, AnyRef](config.OLD_ID -> event.oldId) else Map[String, AnyRef]()}})
        logger.info(s"generateActivityCertificate: Adding certificate to registry for userId=${certModel.identifier}, uuid=$uuid")
        addCertToRegistry(event, addReq, context)(metrics)
        val related = event.related
        val userEnrollmentData = UserEnrollmentData(related.getOrElse(config.BATCH_ID, "").asInstanceOf[String], certModel.identifier,
          "", // no courseId for activity context
          if (StringUtils.isNotBlank(event.activityName)) event.activityName else event.courseName,
          event.templateId,
          Certificate(uuid, event.name, qrMap.accessCode, formatter.format(new Date()), "", ""),
          activityId = Option(related.getOrElse(config.ACTIVITY_ID, "").asInstanceOf[String]),
          activityType = Option(related.getOrElse(config.ACTIVITY_TYPE, "").asInstanceOf[String]),
          activityName = Option(event.activityName)
        )
        logger.info(s"generateActivityCertificate: Updating user enrollment table for userId=${certModel.identifier}, batchId=${related.getOrElse(config.BATCH_ID, "")}, activityId=${related.getOrElse(config.ACTIVITY_ID, "")}, activityType=${related.getOrElse(config.ACTIVITY_TYPE, "")}")
        updateUserEnrollmentTable(event, userEnrollmentData, context)
        metrics.incCounter(config.successEventCount)
        logger.info(s"generateActivityCertificate: Certificate generation completed for userId=${certModel.identifier}, uuid=$uuid")
      } finally {
        cleanUp(uuid, directory)
      }
    })
  }

  @throws[Exception]
  def generateActivityCertificateUsingRC(event: Event, context: KeyedProcessFunction[String, Event, String]#Context)(implicit metrics: Metrics): Unit = {
    logger.info(s"generateActivityCertificateUsingRC: Generating certificate for userId=${event.eData.getOrElse("userId", "")}, activityType=${event.eData.getOrElse("activityType", "")}, activityName=${event.activityName}, batchId=${event.eData.getOrElse("batchId", "")}")
    val certModelList: List[CertModel] = new CertMapper(certificateConfig).mapReqToCertModel(event)
    certModelList.foreach(certModel => {
      var uuid: String = null
      val reIssue: Boolean = event.oldId.nonEmpty
      if(reIssue){
        try { callCertificateRc(config.rcDeleteApi, event.oldId, null) } catch {
          case ex: ServerException =>
            logger.error("Rc deletion failed | old id is not present :: identifier " + event.oldId + " :: " + ex.getMessage)
            deleteOldRegistry(event.oldId)
          case e: UnirestException => logger.error("Rc deletion failed due to connection :: identifier " + event.oldId + " :: " + e.getMessage)
        }
      }
      logger.info(s"generateActivityCertificateUsingRC: Generating CertModel for userId=${certModel.identifier}, recipientName=${certModel.recipientName}, certificateName=${certModel.certificateName}")
      val related = event.related
      val certReq = generateActivityRequest(event, certModel, reIssue)
      uuid = callCertificateRc(config.rcCreateApi, null, certReq)
      logger.info(s"generateActivityCertificateUsingRC: Certificate RC created for userId=${certModel.identifier}, uuid=$uuid")
      val userEnrollmentData = UserEnrollmentData(related.getOrElse(config.BATCH_ID, "").asInstanceOf[String], certModel.identifier,
        "",
        if (StringUtils.isNotBlank(event.activityName)) event.activityName else event.courseName,
        event.templateId,
        Certificate(uuid, event.name, "", formatter.format(new Date()), event.svgTemplate, config.rcEntity),
        activityId = Option(related.getOrElse(config.ACTIVITY_ID, "").asInstanceOf[String]),
        activityType = Option(event.eData.getOrElse(config.ACTIVITY_TYPE, "").asInstanceOf[String]),
        activityName = Option(event.activityName)
      )
      logger.info(s"generateActivityCertificateUsingRC: Updating user enrollment table for userId=${certModel.identifier}, batchId=${related.getOrElse(config.BATCH_ID, "")}, activityId=${related.getOrElse(config.ACTIVITY_ID, "")}, activityType=${related.getOrElse(config.ACTIVITY_TYPE, "")}")
      updateUserEnrollmentTable(event, userEnrollmentData, context)
      metrics.incCounter(config.successEventCount)
      logger.info(s"generateActivityCertificateUsingRC: Certificate generation completed for userId=${certModel.identifier}, uuid=$uuid")
    })
  }

  @throws[Exception]
  def generateCertificateUsingRC(event: Event, context: KeyedProcessFunction[String, Event, String]#Context)(implicit metrics: Metrics): Unit = {
    if (event.isActivity) generateActivityCertificateUsingRC(event, context)
    else generateCourseCertificateUsingRC(event, context)
  }

  private def resolveActivityValForRequest(isActivity: Boolean, activityType: String, batchId: String, courseId: String): Option[String] = {
    try {
      if (!isActivity) {
        if (StringUtils.isNotBlank(courseId) && StringUtils.containsIgnoreCase(batchId, courseId))
          Option(getActivityValueFromEs(batchId)).filter(StringUtils.isNotBlank)
        else None
      } else {
        if (StringUtils.equalsIgnoreCase(activityType, "Competency Level"))
          Option(getActivityValueFromEs(batchId)).filter(StringUtils.isNotBlank)
        else None
      }
    } catch {
      case _: Throwable => None
    }
  }

  def generateCourseRequest(event: Event, certModel: CertModel, reIssue: Boolean): Map[String, AnyRef] = {
    val req = Map("filters" -> Map())
    val batchId = event.related.getOrElse(config.BATCH_ID, "").asInstanceOf[String]
    val courseId = event.related.getOrElse(config.COURSE_ID, "").asInstanceOf[String]
    val publicKeyId: String = callCertificateRc(config.rcSearchApi, null, req)
    val replacedUrl = if(event.svgTemplate.contains(config.cloudStoreBasePathPlaceholder)) event.svgTemplate.replace(config.cloudStoreBasePathPlaceholder, config.baseUrl+"/"+config.contentCloudStorageContainer) else event.svgTemplate
    logger.info("generateCourseRequest: template url from event {}", event.svgTemplate)
    logger.info("generateCourseRequest: template url after replacing placeholder {}", replacedUrl)

    val trainingPayload = if (StringUtils.containsIgnoreCase(batchId, courseId)) {
      val activityVal = resolveActivityValForRequest(isActivity = false, activityType = null, batchId = batchId, courseId = courseId)
      Training(courseId, event.courseName, "Course", batchId, None, event.issuedDate, activity = activityVal)
    } else {
      val learnerProfile = Option.apply(getLearnerProfile(courseId, batchId))
      Training(courseId, event.courseName, "Course", batchId, learnerProfile, event.issuedDate, activity = None)
    }

    val createCertReq = Map[String, AnyRef](
      "certificateLabel" -> certModel.certificateName,
      "status" -> "ACTIVE",
      "templateUrl" -> replacedUrl,
      "training" -> trainingPayload,
      "recipient" -> Recipient(certModel.identifier, certModel.recipientName, null),
      "issuer" -> Issuer(certModel.issuer.url, certModel.issuer.name, publicKeyId),
      "signatory" -> event.signatoryList,
    ) ++ {if (reIssue) Map[String, AnyRef](config.OLD_ID -> event.oldId) else Map[String, AnyRef]()}
    createCertReq
  }

  def generateActivityRequest(event: Event, certModel: CertModel, reIssue: Boolean): Map[String, AnyRef] = {
    val req = Map("filters" -> Map())
    val batchId = event.related.getOrElse(config.BATCH_ID, "").asInstanceOf[String]
    val activityId = event.related.getOrElse(config.ACTIVITY_ID, "").asInstanceOf[String]
    val activityType = event.eData.getOrElse("activityType", "").asInstanceOf[String]
    val displayName = if (StringUtils.isNotBlank(event.activityName)) event.activityName else event.courseName
    val publicKeyId: String = callCertificateRc(config.rcSearchApi, null, req)
    val replacedUrl = if(event.svgTemplate.contains(config.cloudStoreBasePathPlaceholder)) event.svgTemplate.replace(config.cloudStoreBasePathPlaceholder, config.baseUrl+"/"+config.contentCloudStorageContainer) else event.svgTemplate
    logger.info("generateActivityRequest: template url from event {}", event.svgTemplate)
    logger.info("generateActivityRequest: template url after replacing placeholder {}", replacedUrl)

    val activityVal = resolveActivityValForRequest(isActivity = true, activityType = activityType, batchId = batchId, courseId = null)

    val createCertReq = Map[String, AnyRef](
      "certificateLabel" -> certModel.certificateName,
      "status" -> "ACTIVE",
      "templateUrl" -> replacedUrl,
      "training" -> Training(activityId, displayName, activityType, batchId, None, event.issuedDate, activity = activityVal),
      "recipient" -> Recipient(certModel.identifier, certModel.recipientName, null),
      "issuer" -> Issuer(certModel.issuer.url, certModel.issuer.name, publicKeyId),
      "signatory" -> event.signatoryList,
    ) ++ {if (reIssue) Map[String, AnyRef](config.OLD_ID -> event.oldId) else Map[String, AnyRef]()}
    createCertReq
  }

  def deleteOldRegistry(id: String): Unit = {
    try {
      deleteCassandraRecord(id)
      deleteEsRecord(id)
    } catch {
      case ex: Exception =>
        logger.error("Old registry deletion failed | old id is not present :: identifier " + id+ " :: " + ex.getMessage)

    }
  }

  def deleteCassandraRecord(id: String): Unit = {
    val query = QueryBuilder.delete().from(config.sbKeyspace, config.certRegTable)
      .where(QueryBuilder.eq("identifier", id))
      .ifExists
    cassandraUtil.executePreparedStatement(query.toString)
  }

  def deleteEsRecord(id: String): Unit = {
    esUtil.deleteDocument(id)
  }

  @throws[ServerException]
  def addCertToRegistry(certReq: Event, request: Map[String, AnyRef], context: KeyedProcessFunction[String, Event, String]#Context)(implicit metrics: Metrics): Unit = {
    logger.info("adding certificate to the registry")
    val httpRequest = ScalaModuleJsonUtils.serialize(request)
    val httpResponse = httpUtil.post(config.certRegistryBaseUrl + config.addCertRegApi, httpRequest)
    if (httpResponse.status == 200) {
      logger.info("certificate added successfully to the registry " + httpResponse.body)
    } else {
      logger.error("certificate addition to registry failed: " + httpResponse.status + " :: " + httpResponse.body)
      throw ServerException("ERR_API_CALL", "Something Went Wrong While Making API Call | Status is: " + httpResponse.status + " :: " + httpResponse.body)
    }
  }

  @throws[ServerException]
  def getLearnerProfile(courseId: String, batchId: String): String = {
    val courseCode = getCourseCode(courseId)
    getLearnerProfileFromBatchOrCourse(batchId, courseCode, courseId)
  }

  private def getCourseCode(courseId: String): String = {
    val requestBody = s"""{
                         |    "request": {
                         |        "filters": {
                         |            "identifier": "$courseId",
                         |            "status": ["Live"]
                         |        },
                         |        "fields": ["code"]
                         |    }
                         |}""".stripMargin
    val response = httpUtil.post(config.searchBaseUrl + config.searchApi, requestBody)
    if (response.status == 200) {
      val responseBody = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
      val result = responseBody.getOrDefault("result", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
      val count = result.getOrDefault("count", 0.asInstanceOf[Number]).asInstanceOf[Number].intValue()
      if (count > 0) {
        val list = result.getOrDefault("content", new java.util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        val courseCode = list.asScala.head.get("code").asInstanceOf[String]
        logger.info(s"Course Code : $courseCode for Course ID: $courseId")
        courseCode
      } else {
        logger.info(s"No Course Code found for Course ID: $courseId, proceeding without Course Code")
        ""
      }
    } else {
      logger.info("search-service error: " + response.body)
      throw new Exception("Something Went Wrong While Making API Call | Status is: " + response.status + " :: " + response.body)
    }
  }
  private def getLearnerProfileFromCourse(courseId: String): String = {
    val requestBody = s"""{
                       |    "request": {
                       |        "filters": {
                       |            "primaryCategory": "Learner Profile",
                       |            "children": ["$courseId"],
                       |            "status": ["Live"]
                       |        },
                       |        "sort_by": {
                       |            "lastPublishedOn": "desc"
                       |        },
                       |        "fields": ["name"]
                       |    }
                       |}""".stripMargin
    val response = httpUtil.post(config.searchBaseUrl + config.searchApi, requestBody)
    if (response.status == 200) {
      val responseBody = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
      val result = responseBody.getOrDefault("result", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
      val count = result.getOrDefault("count", 0.asInstanceOf[Number]).asInstanceOf[Number].intValue()
      if (count > 0) {
        val list = result.getOrDefault("content", new java.util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        val learnerProfile = list.asScala.head.get("name").asInstanceOf[String]
        logger.info("Learner Profile : "+learnerProfile)
        learnerProfile
      } else {
        logger.info(s"No learner profile found for course ID: $courseId, proceeding without learner profile")
        ""
      }
    } else {
      logger.info("search-service error: " + response.body)
      throw new Exception("Something Went Wrong While Making API Call | Status is: " + response.status + " :: " + response.body)
    }
  }

  private def getLearnerProfileFromBatchOrCourse(batchId: String, courseCode: String, courseId: String): String = {
    val requestBody = s"""{
                          |    "request": {
                          |        "filters": {
                          |            "identifier": ["$batchId"],
                          |            "status": [0,1]
                          |        },
                          |        "fields": ["name"]
                          |    }
                          |}""".stripMargin
    val response = httpUtil.post(config.lmsBaseUrl + config.batchSearchApi, requestBody)
    if (response.status == 200) {
      val responseBody = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
      val result = responseBody.getOrDefault("result", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
      val responseMap = result.getOrDefault("response", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
      val count = responseMap.getOrDefault("count", 0.asInstanceOf[Number]).asInstanceOf[Number].intValue().asInstanceOf[Integer]
      logger.info(s"Batch search result: $responseMap, count: $count" + "count class type : "+count.getClass)
      if (count > 0) {
        val list = responseMap.getOrDefault("content", new java.util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        val batchName = list.asScala.head.get("name").asInstanceOf[String]
        logger.info(s"Fetched batchName: '$batchName' for batchId: '$batchId', courseId: '$courseId', response: $responseBody")
        val learnerProfileCode = if (courseCode.nonEmpty && batchName.startsWith(courseCode + "_")) {
            batchName.substring(courseCode.length + 1)
          }
          else {
            getLearnerProfileFromCourse(courseId)
          }
        logger.info("Learner Profile Code: " + learnerProfileCode)
        learnerProfileCode
      } else {
        logger.info(s"No learner profile found for batch ID: $batchId, proceeding without learner profile")
        ""
      }
    } else {
      logger.info("search-service error: " + response.body)
      throw new Exception("Something Went Wrong While Making API Call | Status is: " + response.status + " :: " + response.body)
    }
  }

  @throws[IOException]
  private def encodeQrCode(file: File): String = {
    val fileContent = FileUtils.readFileToByteArray(file)
    file.delete
    Base64.getEncoder.encodeToString(fileContent)
  }

  @throws[IOException]
  private def uploadJson(certificateExtension: CertificateExtension, fileName: String, cloudPath: String): String = {
    logger.info("uploadJson: uploading json file started {}", fileName)
    val file = new File(fileName)
    ScalaModuleJsonUtils.writeToJsonFile(file, certificateExtension)
    storageService.uploadFile(cloudPath, file)
  }

  // Deprecated by context-specific methods; kept for compatibility where used elsewhere
  def generateRequest(event: Event, certModel: CertModel, reIssue: Boolean):  Map[String, AnyRef] = {
    generateCourseRequest(event, certModel, reIssue)
  }

  @throws[ServerException]
  @throws[UnirestException]
  def callCertificateRc(api: String, identifier: String, request: Map[String, AnyRef]): String = {
    logger.info("CertificateGeneratorFunction:: callCertificateRc:: Certificate rc called | Api:: " + api)
    var id: String = null
    val uri: String = config.rcBaseUrl + "/" + config.rcEntity
    val status = api match {
      case config.rcDeleteApi => logger.info("CertificateGeneratorFunction:: callCertificateRc:: RC Delete API - identifier: " + identifier)
        httpUtil.delete(uri + "/" +identifier).status
      case config.rcCreateApi =>
        val plainReq: String = ScalaModuleJsonUtils.serialize(request)
        val req = removeBadChars(plainReq)
        logger.info("CertificateGeneratorFunction:: callCertificateRc:: RC Create API request: " + req)
        val headers = Map[String, String](
          "Content-Type" -> "application/json",
          "Authorization" -> config.rcApiKey
        )
        val httpResponse = httpUtil.post(uri, req, headers)
        if(httpResponse.status == 200) {
          val response = ScalaJsonUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
          logger.info("CertificateGeneratorFunction:: callCertificateRc:: RC Create API response: " + response)
          id = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse(config.rcEntity, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("osid","").asInstanceOf[String]
        } else {
          logger.error("CertificateGeneratorFunction:: callCertificateRc:: RC Create Error Response: " + httpResponse.status +  " :: Response: " + httpResponse.body)
        }
        httpResponse.status
      case config.rcSearchApi =>
        val req: String = ScalaModuleJsonUtils.serialize(request)
        logger.info("CertificateGeneratorFunction:: callCertificateRc:: RC Search API request: " + req)
        val searchUri = config.rcBaseUrl + "/" + "PublicKey" + "/search"
        val httpResponse = httpUtil.post(searchUri, req)
        if(httpResponse.status == 200) {
          val resp = ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](httpResponse.body)
          id = resp.head.getOrElse("osid", null).asInstanceOf[String]
        }
        httpResponse.status
    }
    if (status == 200) {
      logger.info("CertificateGeneratorFunction:: callCertificateRc:: certificate rc successfully executed for api: " + api)
    } else {
      logger.error("CertificateGeneratorFunction:: callCertificateRc:: certificate rc failed for api: " + api +  " | Status is: " + status)
      throw ServerException("ERR_API_CALL", "Something Went Wrong While Making API Call:  " + api +  " | Status is: " + status)
    }
    id
  }

  private def removeBadChars(request: String): String = {
    config.badCharList.split(",").foldLeft(request)((curReq, removeChar) => StringUtils.remove(curReq, removeChar))
  }

  private def cleanUp(fileName: String, path: String): Unit = {
    try {
      val directory = new File(path)
      val files: Array[File] = directory.listFiles
      if (files != null && files.length > 0)
        files.foreach(file => {
          if (file.getName.startsWith(fileName)) file.delete
        })
      logger.info("cleanUp completed")
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
    }
  }

  def updateUserEnrollmentTable(event: Event, certMetaData: UserEnrollmentData, context: KeyedProcessFunction[String, Event, String]#Context)(implicit metrics: Metrics): Unit = {
    logger.info("CertificateGeneratorFunction:: updateUserEnrollmentTable:: event:: ", event)
    logger.info("CertificateGeneratorFunction:: updateUserEnrollmentTable:: certMetaData:: ", certMetaData)
    val isActivity = event.isActivity
    val primaryFields = if (!isActivity) {
      Map(config.userId.toLowerCase() -> certMetaData.userId, config.batchId.toLowerCase -> certMetaData.batchId, config.courseId.toLowerCase -> certMetaData.courseId)
    } else {
      Map(config.userId.toLowerCase() -> certMetaData.userId,
        config.batchId.toLowerCase -> certMetaData.batchId,
        config.dbActivityId -> certMetaData.activityId.getOrElse(""),
        config.dbActivityType -> certMetaData.activityType.getOrElse("")
      )
    }
    val records = getIssuedCertificatesFromUserEnrollmentTable(primaryFields, isActivity)
    if (records.nonEmpty) {
      records.foreach((row: Row) => {
        val issuedOn = row.getTimestamp("completedOn")
        var certificatesList = row.getList(config.issued_certificates, TypeTokens.mapOf(classOf[String], classOf[String]))
        if (certificatesList == null || certificatesList.isEmpty) {
          certificatesList = new util.ArrayList[util.Map[String, String]]()
        }

        val updatedCerts: util.List[util.Map[String, String]] = certificatesList.stream().filter(cert => !StringUtils.equalsIgnoreCase(certMetaData.certificate.name, cert.get("name"))).collect(Collectors.toList())
        updatedCerts.add(mapAsJavaMap(Map[String, String](
          config.name -> certMetaData.certificate.name,
          config.identifier -> certMetaData.certificate.id,
          config.token -> certMetaData.certificate.token,
        ) ++ {if(certMetaData.certificate.lastIssuedOn.nonEmpty) Map[String, String](config.lastIssuedOn -> certMetaData.certificate.lastIssuedOn)
        else Map[String, String]()}
          ++ {if(config.enableRcCertificate) Map[String, String](config.templateUrl -> certMetaData.certificate.templateUrl, config.`type`->certMetaData.certificate.`type`)
        else Map[String, String]()}
        ))

        val query = if (!isActivity)
          getUpdateIssuedCertQuery(updatedCerts, certMetaData.userId, certMetaData.courseId, certMetaData.batchId, config)
        else
          getUpdateIssuedCertQueryForActivity(updatedCerts, certMetaData.userId, certMetaData.activityId.getOrElse(""), certMetaData.activityType.getOrElse(""), certMetaData.batchId, config)

        logger.info("CertificateGeneratorFunction:: updateUserEnrollmentTable:: update query:: ", query.toString)
        val result = cassandraUtil.update(query)
        logger.info("CertificateGeneratorFunction:: updateUserEnrollmentTable:: update result:: ", result)
        if (result) {
          logger.info("issued certificates in user-enrollment table  updated successfully")
          metrics.incCounter(config.dbUpdateCount)
          val certificateAuditEvent = generateAuditEvent(certMetaData, isActivity)
          logger.info("pushAuditEvent: audit event generated for certificate : " + certificateAuditEvent)
          val audit = ScalaJsonUtil.serialize(certificateAuditEvent)
          context.output(config.auditEventOutputTag, audit)
          logger.info("pushAuditEvent: certificate audit event success {}", audit)
          val displayName = if (isActivity) certMetaData.activityName.getOrElse(certMetaData.courseName) else certMetaData.courseName
          val entityId = if (isActivity) certMetaData.activityId.getOrElse("") else certMetaData.courseId
          context.output(config.notifierOutputTag, NotificationMetaData(certMetaData.userId, displayName, issuedOn, entityId, certMetaData.batchId, certMetaData.templateId, event.partition, event.offset, isActivity))
          context.output(config.userFeedOutputTag, UserFeedMetaData(certMetaData.userId, displayName, issuedOn, entityId, event.partition, event.offset))
        } else {
          metrics.incCounter(config.failedEventCount)
          throw new Exception(s"Update certificates to enrolments failed: $event")
        }

      })
    }

  }


  /**
   * returns query for updating issued_certificates in user_enrollment table
   */
  def getUpdateIssuedCertQuery(updatedCerts: util.List[util.Map[String, String]], userId: String, courseId: String, batchId: String, config: CertificateGeneratorConfig):
  Update.Where = QueryBuilder.update(config.dbKeyspace, config.dbEnrollmentTable).where()
    .`with`(QueryBuilder.set(config.issued_certificates, updatedCerts))
    .where(QueryBuilder.eq(config.userId.toLowerCase, userId))
    .and(QueryBuilder.eq(config.courseId.toLowerCase, courseId))
    .and(QueryBuilder.eq(config.batchId.toLowerCase, batchId))

  def getUpdateIssuedCertQueryForActivity(updatedCerts: util.List[util.Map[String, String]], userId: String, activityId: String, activityType: String, batchId: String, config: CertificateGeneratorConfig):
  Update.Where = QueryBuilder.update(config.activityDbKeyspace, config.activityDbEnrollmentTable).where()
    .`with`(QueryBuilder.set(config.issued_certificates, updatedCerts))
    .where(QueryBuilder.eq(config.userId.toLowerCase, userId))
    .and(QueryBuilder.eq(config.dbActivityId, activityId))
    .and(QueryBuilder.eq(config.dbActivityType, activityType))
    .and(QueryBuilder.eq(config.batchId.toLowerCase, batchId))


  private def getIssuedCertificatesFromUserEnrollmentTable(columns: Map[String, AnyRef], isActivity: Boolean)(implicit metrics: Metrics) = {
    logger.info("primary columns {}", columns)
    val selectWhere = QueryBuilder.select().all()
      .from(if (isActivity) config.activityDbKeyspace else config.dbKeyspace, if (isActivity) config.activityDbEnrollmentTable else config.dbEnrollmentTable).
      where()
    columns.map(col => {
      col._2 match {
        case value: List[Any] =>
          selectWhere.and(QueryBuilder.in(col._1, value.asJava))
        case _ =>
          selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })
    logger.info("select query {}", selectWhere.toString)
    metrics.incCounter(config.enrollmentDbReadCount)
    cassandraUtil.find(selectWhere.toString).asScala.toList
  }


  private def generateAuditEvent(data: UserEnrollmentData, isActivity: Boolean = false): CertificateAuditEvent = {
    val rollupId = if (isActivity) data.activityId.getOrElse(data.courseId) else data.courseId
    val env = if (isActivity) "Activity" else "Course"
    CertificateAuditEvent(
      actor = Actor(id = data.userId),
      context = EventContext(env = env, cdata = Array(Map("type" -> (if (isActivity) "ActivityBatch" else config.courseBatch), config.id -> data.batchId).asJava)),
      `object` = EventObject(id = data.certificate.id, `type` = "Certificate", rollup = Map(config.l1 -> rollupId).asJava))
  }

  private def getActivityValueFromEs(batchId: String): String = {
    try {
      val url = s"${config.activityEsBaseUrl}/${config.activityBatchIndex}/_search"
      val payload = s"""{
                    |  "_source": ["name"],
                    |  "query": { "term": { "batchId.raw": "$batchId" } }
                    |}""".stripMargin
      logger.info(s"getActivityValueFromEs: POST $url payload $payload")
      val response = httpUtil.post(url, payload, Map("Content-Type" -> "application/json"))
      if (response.status == 200 && StringUtils.isNotBlank(response.body)) {
        val respMap = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
        val hits = respMap.getOrDefault("hits", new java.util.HashMap[String, AnyRef]).asInstanceOf[java.util.Map[String, AnyRef]]
        val innerHits = hits.getOrDefault("hits", new java.util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        if (!innerHits.isEmpty) {
          val first = innerHits.get(0)
          val source = first.getOrDefault("_source", new java.util.HashMap[String, AnyRef]).asInstanceOf[java.util.Map[String, AnyRef]]
          val name = Option(source.get("name")).map(_.asInstanceOf[String]).getOrElse("")
          logger.info(s"getActivityValueFromEs: resolved activity value '$name' for batchId '$batchId'")
          return name
        }
      } else {
        logger.warn(s"getActivityValueFromEs: non-200 or empty body status=${response.status} body=${response.body}")
      }
    } catch {
      case ex: Exception =>
        logger.error(s"getActivityValueFromEs: failed to fetch activity value for batchId $batchId due to ${ex.getMessage}")
    }
    ""
  }

}