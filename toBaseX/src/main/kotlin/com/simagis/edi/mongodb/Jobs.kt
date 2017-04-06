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
import java.io.PrintWriter
import java.io.StringWriter
import java.util.*
import java.util.concurrent.locks.ReentrantLock
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
    lateinit var claimsAPI: MongoDatabase
    lateinit var claims: MongoDatabase

    val apiJobs: DocumentCollection by lazy { claimsAPI.getCollection("apiJobs") }
    val apiJobsLog: DocumentCollection by lazy { claimsAPI.getCollection("apiJobsLog") }
    val jobFilter get() = doc(jobId)
    val jobDoc: Document? get() = apiJobs.find(jobFilter).first()

    internal fun open(args: Array<String>) {
        val commandLine = com.berryworks.edireader.util.CommandLine(args)
        host = commandLine["host"] ?: "localhost"
        jobId = commandLine["job"] ?: throw IllegalArgumentException("argument -job required")
        val mongoClient = MDBCredentials.mongoClient(host)
        claimsAPI = mongoClient.getDatabase("claimsAPI")
        claims = mongoClient.getDatabase(commandLine["db"] ?: "claims")
        job = this
        logger = this::log
    }

    override fun toString(): String = "${host}/${claims.name}"

    private val printLogLock = ReentrantLock()
    private fun log(
            level: LogLevel,
            message: String,
            error: Throwable? = null,
            details: String? = null,
            detailsJson: Any? = null,
            detailsXml: String? = null) {
        val now = Date()
        fun printLog(message: String, _id: Any?) {
            "${level.toString().padEnd(7)} $message at $now${_id?.let { """ log: ObjectId("$_id")""" } ?: ""}".also {
                val detailsPP = if (detailsJson is Document)
                    detailsJson.toJson(JsonWriterSettings(JsonMode.SHELL, true)) else
                    null
                printLogLock.withLock {
                    System.err.println(it)
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
                    `+`("level", level.value)
                    `+`("msg", message)
                    `+`("time", now)
                    if (error != null) {
                        appendError(error)
                    }
                    if (details != null) {
                        `+`("details", details)
                    }
                    if (detailsJson != null) {
                        `+`("detailsJson", detailsJson)
                    }
                    if (detailsXml != null) {
                        `+`("detailsXml", detailsXml)
                    }
                }.let {
                    apiJobsLog.insertOne(it)
                    printLog(message, it._id)
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

val DocumentCollection.isExists: Boolean get() = namespace.let {
    job.let { job ->
        if (job == null) throw AssertionError("Invalid job")
        if (it.databaseName != job.claims.name) throw AssertionError("Invalid collection: $it")
        job.claims.listCollectionNames().contains(it.collectionName)
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
        detailsXml: String? = null): Unit
{
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
