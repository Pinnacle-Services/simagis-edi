package com.simagis.edi.mongodb

import com.berryworks.edireader.EDISyntaxException
import com.mongodb.ErrorCategory
import com.mongodb.MongoClient
import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.simagis.edi.basex.ISA
import com.simagis.edi.basex.exit
import com.simagis.edi.basex.get
import org.basex.core.Context
import org.basex.core.MainOptions
import org.basex.core.cmd.CreateDB
import org.basex.core.cmd.Replace
import org.basex.core.cmd.XQuery
import org.bson.BsonSerializationException
import org.bson.Document
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import javax.json.Json
import javax.json.JsonArray
import javax.json.JsonObject
import kotlin.concurrent.withLock
import kotlin.system.exitProcess

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/9/2017.
 */
fun main(args: Array<String>) {
    val commandLine = com.berryworks.edireader.util.CommandLine(args)
    val host = commandLine["host"] ?: "localhost"
    val xq = commandLine["xq"] ?: File("claims.xq").absolutePath
    val db = commandLine["db"] ?: "claims"
    val collection = commandLine["c"] ?: "claims"
    val mode = commandLine["code"] ?: "R"

    if (commandLine.size() == 0)
        exit("""
        Usage: ToMongoDB.kt [-host host] [-xq file.xq] [-db database] [-c collection] [-mode R|D|F] <path>
            host: $host
            xq: $xq
            database: $db
            collection: $collection
            mode: $mode""")

    val xqText = File(xq).readText()

    val mongoClient = MongoClient(host)
    val mongoDBs: ThreadLocal<MongoDatabase> = ThreadLocal.withInitial { mongoClient.getDatabase(db) }
    val mongoOut: ThreadLocal<MongoCollection<Document>> = ThreadLocal.withInitial { mongoDBs.get().getCollection(collection) }
    val mongoLog: ThreadLocal<MongoCollection<Document>> = ThreadLocal.withInitial { mongoDBs.get().getCollection("claimsLog") }

    val printLogLock = ReentrantLock()
    fun log(level: String, message: String, error: Throwable? = null, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) {
        val now = Date()
        fun printLog(message: String, _id: Any?) {
            """[${level.padEnd(8)}] $message at $now log: ObjectId("$_id}")""".also {
                printLogLock.withLock {
                    System.err.println(it)
                    error?.printStackTrace()
                }
            }
        }
        try {
            val document = Document().apply {
                append("level", level)
                append("msg", message)
                append("time", now)
                if (error != null) {
                    append("error", Document()
                            .append("class", error.javaClass.name)
                            .append("message", error.message)
                            .append("stackTrace", StringWriter().let {
                                PrintWriter(it).use { error.printStackTrace(it) }
                                it.toString()
                            })
                    )
                }
                if (details != null) {
                    append("details", details)
                }
                if (detailsJson != null) {
                    append("detailsJson", detailsJson)
                }
                if (detailsXml != null) {
                    append("detailsXml", detailsXml)
                }
            }
            mongoLog.get().insertOne(document)
            printLog(message, document["_id"])
        } catch(e: Throwable) {
            e.printStackTrace()
            printLog("${e.javaClass.simpleName} on $message", null)
            if (e !is BsonSerializationException) {
                exit("Logging error: ${e.message}")
            }
        }
    }

    fun info(message: String, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
            level = "INFO",
            message = message,
            details = details,
            detailsJson = detailsJson,
            detailsXml = detailsXml
    )

    fun warning(message: String, error: Throwable? = null, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
            level = "WARNING",
            message = message,
            error = error,
            details = details,
            detailsJson = detailsJson,
            detailsXml = detailsXml
    )

    fun error(message: String, error: Throwable? = null, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
            level = "ERROR",
            message = message,
            error = error,
            details = details,
            detailsJson = detailsJson,
            detailsXml = detailsXml
    )

    val inMemoryBaseX: ThreadLocal<Context> = ThreadLocal.withInitial {
        Context().apply {
            options.set(MainOptions.MAINMEM, true)
            CreateDB("memory").execute(this)
        }
    }

    val path = File(commandLine[0])

    fun ISA.toClaimsJsonArray(): JsonArray? {
        fun invalidISA(isa: ISA, e: Exception? = null) = warning(
                "Invalid ISA: ${isa.stat}",
                e,
                details = isa.code,
                detailsXml = if (e is EDISyntaxException) try {
                    isa.toXML().toString(ISA.CHARSET)
                } catch(e: Exception) {
                    null
                } else null
        )

        if (valid) try {
            val context = inMemoryBaseX.get()

            with(Replace("doc")) {
                setInput(toXML().inputStream())
                execute(context)
            }

            return with(XQuery(xqText)) {
                val json = execute(context)
                Json.createReader(json.reader()).readArray()
            }
        } catch(e: Exception) {
            invalidISA(this, e)
        } else {
            invalidISA(this, null)
        }
        return null
    }

    val fileCount = AtomicLong()
    val fileCountInvalid = AtomicLong()
    val isaCount = AtomicLong()
    val isaCountInvalid = AtomicLong()
    val claimCount = AtomicLong()
    val claimCountInvalid = AtomicLong()
    val claimCountDuplicate = AtomicLong()
    fun details() = """
        DETAILS:
            file: ${fileCount.get()}
            file invalid: ${fileCountInvalid.get()}
            isa: ${isaCount.get()}
            isa invalid: ${isaCountInvalid.get()}
            claim: ${claimCount.get()}
            claim duplicate: ${claimCountDuplicate.get()}
            claim invalid: ${claimCountInvalid.get()}
"""
    try {
        info("Scanning $path...")
        val listFiles = when (mode) {
            "R" -> path.walk().toList()
            "D" -> path.listFiles()?.asList() ?: emptyList()
            "F" -> listOf(path)
            else -> emptyList()
        }.filter {
            it.isFile
        }

        fun Document.prepare(): Document {
            val DT8 by lazy { SimpleDateFormat("YYYYMMDD") }
            val DT6 by lazy { SimpleDateFormat("YYYYMM") }
            val DT4 by lazy { SimpleDateFormat("YYYYMM") }
            val _id = remove("id")
            append("_id", _id)
            fun Document.fixTypes() {
                fun String.isTyped() = contains('-')
                fun String.type() = substringAfter('-')
                fun String.name() = substringBefore('-')
                keys.toList().forEach { key ->
                    try {
                        when (key.type()) {
                            "I" -> getString(key)?.also { value -> append(key.name(), value.toInt()) }
                            "L" -> getString(key)?.also { value -> append(key.name(), value.toLong()) }
                            "F" -> getString(key)?.also { value -> append(key.name(), value.toDouble()) }
                            "DT8" -> getString(key)?.also { value ->
                                when (value.length) {
                                    8 -> append(key.name(), DT8.parse(value))
                                    6 -> append(key.name(), DT6.parse(value))
                                    4 -> append(key.name(), DT4.parse(value))
                                    else -> {
                                        warning("Invalid DT8 value at $_id $key: ${getString(key)}", detailsJson = this)
                                    }
                                }
                            }
                        }
                    } catch(e: Exception) {
                        when (e) {
                            is ParseException -> warning("Invalid date format at $_id $key: ${getString(key)}", detailsJson = this)
                            is NumberFormatException -> warning("Invalid number format at $_id $key: ${getString(key)}", detailsJson = this)
                            else -> throw e
                        }
                    }
                }
                keys.removeIf(String::isTyped)
                values.forEach {
                    when (it) {
                        is Document -> it.fixTypes()
                        is List<*> -> it.forEach { if (it is Document) it.fixTypes() }
                    }
                }
            }
            fixTypes()
            return this
        }

        info("Starting import $path into $host $db $collection")
        listFiles.stream().parallel().forEach { file ->
            val isaList: List<ISA>
            try {
                isaList = ISA.read(file)
                fileCount.incrementAndGet()
            } catch(e: Exception) {
                fileCountInvalid.incrementAndGet()
                warning("Invalid file $file", e)
                return@forEach
            }
            isaList.forEach { isa ->
                val claims = isa.toClaimsJsonArray()
                if (claims != null) {
                    isaCount.incrementAndGet()
                    claims.forEach {
                        if (it is JsonObject) {
                            val document = Document.parse(it.toString()).prepare()
                            try {
                                mongoOut.get().insertOne(document)
                                claimCount.incrementAndGet()
                            } catch(e: Throwable) {
                                if (e is MongoWriteException && ErrorCategory.fromErrorCode(e.code)
                                        == ErrorCategory.DUPLICATE_KEY) {
                                    claimCountDuplicate.incrementAndGet()
                                    warning("DUPLICATE", detailsJson = document)
                                } else {
                                    claimCountInvalid.incrementAndGet()
                                    warning("insertOne error: ${e.message}", e,
                                            detailsJson = document)
                                }

                            }
                        }
                    }
                } else {
                    isaCountInvalid.incrementAndGet()
                }
            }
        }
        info("DONE for $path into $host $db $collection ${details()}")
    } catch(e: Throwable) {
        error("ERROR: ${e.message} ${details()}", e)
        exitProcess(2)
    }
}
