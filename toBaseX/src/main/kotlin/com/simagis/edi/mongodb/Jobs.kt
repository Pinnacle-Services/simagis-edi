package com.simagis.edi.mongodb

import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.simagis.edi.basex.exit
import com.simagis.edi.basex.get
import com.simagis.edi.mdb.*
import org.bson.BsonSerializationException
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonWriterFactory
import javax.json.stream.JsonGenerator
import kotlin.concurrent.withLock

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 4/3/2017.
 */
typealias DocumentCollection = MongoCollection<Document>

private var job: AbstractJob? = null

internal abstract class AbstractJob {
    lateinit var host: String
    lateinit var jobId: String
    lateinit var dbs: JobDBS
    lateinit var claimsAPI: MongoDatabase
    lateinit var claims: MongoDatabase
    lateinit var claimsA: MongoDatabase
    lateinit var dictionary: MongoDatabase

    val apiJobs: DocumentCollection by lazy { claimsAPI.getCollection("apiJobs") }
    val apiJobsLog: DocumentCollection by lazy { claimsAPI.getCollection("apiJobsLog") }
    val jobFilter get() = doc(jobId)
    val jobDoc: Document? get() = apiJobs.find(jobFilter).first()
    val jobLogDir: File by lazy { File(".").resolve("logs").also { it.mkdir() }.resolve(jobId).also { it.mkdir() } }
    val jobLogTxtDir: File by lazy { jobLogDir.resolve("txt").also { it.mkdir() } }
    val jobLogXmlDir: File by lazy { jobLogDir.resolve("xml").also { it.mkdir() } }
    val jobLogJsonDir: File by lazy { jobLogDir.resolve("json").also { it.mkdir() } }

    internal fun open(args: Array<String>) {
        val commandLine = com.berryworks.edireader.util.CommandLine(args)
        host = commandLine["host"] ?: "localhost"
        jobId = commandLine["job"] ?: throw IllegalArgumentException("argument -job required")
        dbs = JobDBS(host)
        claimsAPI = dbs["claimsAPI"]
        claims = dbs[commandLine["db"] ?: "claims"]
        claimsA = dbs[commandLine["dbA"] ?: "claimsA"]
        dictionary = dbs[commandLine["dbDictionary"] ?: "dictionary"]
        job = this
        logger = this::log
    }

    override fun toString(): String = "${host}/${claims.name}"

    private val jsonWriterSettingsPPM by lazy { JsonWriterSettings(JsonMode.SHELL, true) }
    private fun Document?.toStringPPM(): String = when {
        this == null -> "{}"
        else -> toJson(jsonWriterSettingsPPM)
    }

    private val jsonPP: JsonWriterFactory = Json.createWriterFactory(mapOf(
            JsonGenerator.PRETTY_PRINTING to true))

    private fun JsonObject?.toStringPP(): String = when {
        this == null -> "{}"
        else -> StringWriter().use { jsonPP.createWriter(it).write(this); it.toString().trim() }
    }

    private val printLogLock = ReentrantLock()
    private fun log(
            level: LogLevel,
            message: String,
            error: Throwable? = null,
            details: String? = null,
            detailsJson: Any? = null,
            detailsXml: String? = null) {
        val now = Date()
        fun printLog(msg: String, _id: Any?) {
            "${level.toString().padEnd(7)} $msg at $now${_id?.let { """ log: ObjectId("$_id")""" } ?: ""}".also {
                val detailsPP = if (detailsJson is Document)
                    detailsJson.toJson(JsonWriterSettings(JsonMode.SHELL, true)) else
                    null
                printLogLock.withLock {
                    System.err.println(it)
                    if (details != null)
                        System.err.println(details)
                    if (detailsPP != null)
                        System.err.println(detailsPP)
                    error?.printStackTrace()
                }
            }
        }

        fun Document.appendError(e: Throwable) {
            `+`("error", doc {
                `+`("errorClass", e.javaClass.name)
                `+`("message", e.message)
                `+`("stackTrace", StringWriter().let {
                    PrintWriter(it).use { e.printStackTrace(it) }
                    it.toString().lines()
                })
            })
        }
        try {
            if (level == TRACE)
                printLog(message, null)
            else
                doc {
                    `+`("job", jobId)
                    `+`("level", level.value)
                    `+`("msg", message)
                    `+`("time", now)
                    if (error != null) {
                        appendError(error)
                    }
                    if (details != null) {
                        `+`("details", true)
                    }
                    if (detailsJson != null) {
                        `+`("detailsJson", true)
                    }
                    if (detailsXml != null) {
                        `+`("detailsXml", true)
                    }
                }.let {
                    apiJobsLog.insertOne(it)
                    printLog(message, it._id)
                    if (details != null) {
                        jobLogTxtDir.resolve("${it._id}.txt").writeText(details)
                    }
                    if (detailsJson != null) {
                        jobLogJsonDir.resolve("${it._id}.json").writeText(when (detailsJson) {
                            is JsonObject -> detailsJson.toStringPP()
                            is Document -> detailsJson.toStringPPM()
                            else -> detailsJson.toString()
                        })
                    }
                    if (detailsXml != null) {
                        jobLogXmlDir.resolve("${it._id}.xml").writeText(detailsXml)
                    }
                }
        } catch(e: Throwable) {
            e.printStackTrace()
            printLog("${e.javaClass.simpleName} on $message", null)
            if (e !is BsonSerializationException) {
                exit("Logging error: ${e.message}")
            }
        }
    }
}

internal fun AbstractJob.updateProcessing(field: String, value: Any?) {
    apiJobs.updateOne(jobFilter, doc { `+$set` { `+`("processing.$field", value) } })
}

internal class JobDBS(host: String) {
    private val mongoClient = MDBCredentials.mongoClient(host)
    private val cache = mutableMapOf<String, MongoDatabase>()

    operator fun get(name: String): MongoDatabase = cache.getOrPut(name) {
        mongoClient.getDatabase(name)
    }
}

val DocumentCollection.isExists: Boolean get() = namespace.let {
    job.let { job ->
        if (job == null) throw AssertionError("Invalid job")
        job.dbs[it.databaseName].listCollectionNames().contains(it.collectionName)
    }
}

var logger: ((LogLevel, String, Throwable?, String?, Any?, String?) -> Unit)? = null

sealed class LogLevel(val value: Int) {
    override fun toString(): String = javaClass.simpleName
}

object TRACE : LogLevel(100)
object INFO : LogLevel(500)
object WARNING : LogLevel(1000)
object ERROR : LogLevel(5000)

@Suppress("NOTHING_TO_INLINE")
inline fun log(
        level: LogLevel, message: String, error: Throwable? = null,
        details: String? = null,
        detailsJson: Any? = null,
        detailsXml: String? = null): Unit {
    logger?.let { it(level, message, error, details, detailsJson, detailsXml) }
}

@Suppress("NOTHING_TO_INLINE")
inline fun trace(message: String, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
        level = TRACE,
        message = message,
        details = details,
        detailsJson = detailsJson,
        detailsXml = detailsXml
)

@Suppress("NOTHING_TO_INLINE")
inline fun info(message: String, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
        level = INFO,
        message = message,
        details = details,
        detailsJson = detailsJson,
        detailsXml = detailsXml
)

@Suppress("NOTHING_TO_INLINE")
inline fun warning(message: String, error: Throwable? = null, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
        level = WARNING,
        message = message,
        error = error,
        details = details,
        detailsJson = detailsJson,
        detailsXml = detailsXml
)

@Suppress("NOTHING_TO_INLINE")
inline fun error(message: String, error: Throwable? = null, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
        level = ERROR,
        message = message,
        error = error,
        details = details,
        detailsJson = detailsJson,
        detailsXml = detailsXml
)
