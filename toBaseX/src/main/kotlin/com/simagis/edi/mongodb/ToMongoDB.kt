package com.simagis.edi.mongodb

import com.berryworks.edireader.EDISyntaxException
import com.mongodb.ErrorCategory
import com.mongodb.MongoNamespace
import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.simagis.edi.basex.ISA
import com.simagis.edi.basex.exit
import com.simagis.edi.basex.get
import com.simagis.edi.mdb.*
import org.basex.core.Context
import org.basex.core.MainOptions
import org.basex.core.cmd.CreateDB
import org.basex.core.cmd.Replace
import org.basex.core.cmd.XQuery
import org.bson.BsonSerializationException
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import org.bson.types.ObjectId
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.security.MessageDigest
import java.text.ParseException
import java.text.SimpleDateFormat
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import javax.json.Json
import javax.json.JsonArray
import javax.json.JsonObject
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import kotlin.system.exitProcess


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/9/2017.
 */

private typealias DocumentCollection = MongoCollection<Document>

private object ImportJob {
    lateinit var host: String
    lateinit var jobId: String
    lateinit var api: MongoDatabase
    lateinit var data: MongoDatabase

    val apiJobs: DocumentCollection by lazy { api.getCollection("apiJobs") }
    val apiJobsLog: DocumentCollection by lazy { api.getCollection("apiJobsLog") }
    val importFiles: DocumentCollection by lazy { api.getCollection("importFiles") }
    val jobFilter get() = doc(jobId)

    val jobDoc: Document? get() = apiJobs.find(jobFilter).first()

    object options {
        private val options: Document by lazy { jobDoc?.get("options") as? Document ?: Document() }
        val sourceDir: File by lazy { (options["sourceDir"] as? String)?.let(::File) ?: File("/DATA/sourceFiles") }
        val scanMode: String by lazy { (options["scanMode"] as? String) ?: "R" }
        val xqDir: File by lazy { (options["xqDir"] as? String)?.let(::File) ?: File("isa-claims-xq") }
        val parallel: Int by lazy { (options["parallel"] as? Int) ?: Runtime.getRuntime().availableProcessors() }
        val after: Date?  by lazy {
            options["after"]?.let {
                when (it) {
                    is Date -> it
                    is String -> java.sql.Date.valueOf(it)
                    else -> null
                }
            }
        }
        val restartMemoryLimit: Long by lazy { (options["restartMemoryLimit"] as? Number)?.toLong() ?: 500 * 1024 * 1024 }
        val restartMaxDurationM: Long by lazy { (options["restartMaxDurationM"] as? Number)?.toLong() ?: 60 }

        object claimTypes {
            private val claimTypes: Document by lazy { options["claimTypes"] as? Document ?: Document() }
            private val cache: MutableMap<String, ClaimType> = ConcurrentHashMap()

            val types: Set<String> get() = claimTypes.keys

            operator fun get(type: String): ClaimType = cache.computeIfAbsent(type) {
                val claimType = (claimTypes[type] as? Document) ?: Document()
                ClaimType(
                        type = type,
                        importTo = claimType["importTo"] as? String ?: "claims_$type.temp",
                        renameTo = claimType["renameTo"] as? String ?: "claims_$type.target",
                        createIndexes = claimType["createIndexes"] as? Boolean ?: true
                )
            }

        }

        data class ClaimType(
                val type: String,
                val importTo: String,
                val renameTo: String,
                val createIndexes: Boolean) {
            val importToCollection: DocumentCollection by lazy { data.getCollection(importTo) }
            val renameToCollection: DocumentCollection by lazy { data.getCollection(renameTo) }
            val renameToCollectionExists: Boolean by lazy { data.listCollectionNames().contains(renameTo) }
        }
    }

    fun open(args: Array<String>) {
        val commandLine = com.berryworks.edireader.util.CommandLine(args)
        host = commandLine["host"] ?: "localhost"
        jobId = commandLine["job"] ?: throw IllegalArgumentException("argument -job required")
        val mongoClient = MDBCredentials.mongoClient(host)
        api = mongoClient.getDatabase("claimsAPI")
        data = mongoClient.getDatabase(commandLine["db"] ?: "claims")
    }

    override fun toString(): String = "$host/${data.name}"
}

fun main(args: Array<String>) {
    ImportJob.open(args)
    info("starting job", detailsJson = ImportJob.jobDoc)
    val importing = MFiles()

    val xqTypes: MutableMap<String, String> = ConcurrentHashMap()

    val digests: MutableSet<String> = Collections.synchronizedSet(mutableSetOf<String>())
    fun File.digest() = inputStream().use { stream ->
        fun md(): MessageDigest = MessageDigest.getInstance("SHA")
        fun ByteArray.toHexString(): String = joinToString(separator = "") {
            Integer.toHexString(it.toInt() and 0xff)
        }
        md().apply {
            val bytes = ByteArray(4096)
            while (true) {
                val len = stream.read(bytes)
                if (len == -1) break
                update(bytes, 0, len)
            }
        }.digest().toHexString()
    }

    val inMemoryBaseX: ThreadLocal<Context> = ThreadLocal.withInitial {
        Context().apply {
            options.set(MainOptions.MAINMEM, true)
            CreateDB("memory").execute(this)
        }
    }


    fun ISA.toClaimsJsonArray(file: File): JsonArray? {
        fun invalidISA(isa: ISA, e: Throwable? = null) = warning(
                "Invalid ISA: ${isa.stat} from $file",
                e,
                details = isa.code,
                detailsXml = if (e !is EDISyntaxException) try {
                    isa.toXML().toString(ISA.CHARSET)
                } catch(e: Exception) {
                    null
                } else null
        )

        if (valid) try {
            val xqText = stat.doc.type?.let { type ->
                xqTypes.getOrPut(type) {
                    ImportJob.options.xqDir.resolve("isa-claims-$type.xq").let { file ->
                        if (file.isFile) file.readText() else {
                            warning("$file not found")
                            ""
                        }
                    }
                }
            } ?: ""

            if (xqText.isEmpty()) {
                return null
            }

            val context = inMemoryBaseX.get()

            with(Replace("doc")) {
                setInput(toXML().inputStream())
                execute(context)
            }

            return with(XQuery(xqText)) {
                val json = execute(context)
                Json.createReader(json.reader()).readArray()
            }
        } catch(e: Throwable) {
            invalidISA(this, e)
        } else {
            invalidISA(this, null)
        }
        return null
    }

    val fileCount = AtomicLong()
    val fileCountDuplicate = AtomicLong()
    val fileCountInvalid = AtomicLong()
    val isaCount = AtomicLong()
    val isaCountInvalid = AtomicLong()
    val claimCount = AtomicLong()
    val claimCountInvalid = AtomicLong()
    val claimCountDuplicate = AtomicLong()
    fun details() = """
        DETAILS:
            file: ${fileCount.get()}
            file duplicate: ${fileCountDuplicate.get()}
            file invalid: ${fileCountInvalid.get()}
            isa: ${isaCount.get()}
            isa invalid: ${isaCountInvalid.get()}
            claim: ${claimCount.get()}
            claim duplicate: ${claimCountDuplicate.get()}
            claim invalid: ${claimCountInvalid.get()}
"""
    try {
        fun Document.prepare(): Document {
            val DT8 by lazy { SimpleDateFormat("yyyyMMdd") }
            val DT6 by lazy { SimpleDateFormat("yyyyMM") }
            val DT4 by lazy { SimpleDateFormat("yyyy") }
            val _id = remove("id")
            append("_id", _id)
            fun Document.fixTypes() {
                fun String.isTyped() = contains('-')
                fun String.type() = substringAfter('-')
                fun String.name() = substringBefore('-')
                keys.toList().forEach { key ->
                    try {
                        fun append(value: String?, cast: (String) -> Any?) {
                            if (value != null && value.isNotBlank())
                                append(key.name(), cast(value))
                        }

                        fun Double.toD0(): Double = if (isFinite()) this else 0.toDouble()

                        fun Double.toC0(): Double = if (isFinite()) "%.2f".format(this).toDouble() else 0.toDouble()

                        when (key.type()) {
                            "I" -> append(getString(key)) { it.toIntOrNull() }
                            "I0" -> append(getString(key)) { it.toIntOrNull() ?: 0 }
                            "L" -> append(getString(key)) { it.toLongOrNull() }
                            "L0" -> append(getString(key)) { it.toLongOrNull() ?: 0.toLong() }

                            "D",
                            "F" -> append(getString(key)) { it.toDoubleOrNull()?.toD0() }
                            "D0",
                            "F0" -> append(getString(key)) { it.toDoubleOrNull()?.toD0() ?: 0.0.toD0() }

                            "C" -> append(getString(key)) { it.toDoubleOrNull()?.toC0() }
                            "C0" -> append(getString(key)) { it.toDoubleOrNull()?.toC0() ?: 0.0.toC0() }

                            "CC" -> append(getString(key)) {
                                it.split("[\\s,;.]".toRegex())
                                        .filter(String::isNotEmpty)
                                        .map(String::toLowerCase)
                                        .map(String::capitalize)
                                        .joinToString(separator = " ")
                            }
                            "DT8" -> getString(key)?.also { value ->
                                when (value.length) {
                                    8 -> append(key.name(), DT8.parse(value))
                                    6 -> append(key.name(), DT6.parse(value))
                                    4 -> append(key.name(), DT4.parse(value))
                                    0 -> Unit
                                    else -> {
                                        warning("Invalid DT8 value at $_id $key: '${getString(key)}'", detailsJson = this)
                                    }
                                }
                            }
                        }
                    } catch(e: Exception) {
                        when (e) {
                            is ParseException -> warning("Invalid date format at $_id $key: '${getString(key)}'", detailsJson = this)
                            is NumberFormatException -> warning("Invalid number format at $_id $key: '${getString(key)}'", detailsJson = this)
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

        fun import(file: File) {
            if (!digests.add(file.digest())) {
                fileCountDuplicate.incrementAndGet()
                info("Duplicate file $file")
                return
            }

            val isaList: List<ISA>
            try {
                isaList = ISA.read(file)
                fileCount.incrementAndGet()
            } catch(e: Exception) {
                fileCountInvalid.incrementAndGet()
                warning("Invalid file $file", e)
                return
            }
            isaList.forEach { isa ->
                val claims = isa.toClaimsJsonArray(file)
                if (claims != null) {
                    isaCount.incrementAndGet()
                    claims.forEach {
                        val claimType = ImportJob.options.claimTypes[isa.type]
                        if (claimType.importTo.isNotBlank() && it is JsonObject) {
                            val collection = claimType.importToCollection
                            val document = Document.parse(it.toString()).prepare()
                            try {
                                fun Document.inRange(start: Date?) = when (isa.type) {
                                    "835" -> getDate("procDate")?.after(start) ?: false
                                    "837" -> getDate("sendDate")?.after(start) ?: false
                                    else -> true
                                }

                                if (document.inRange(ImportJob.options.after)) {
                                    collection.insertOne(document)
                                    claimCount.incrementAndGet()
                                }
                            } catch(e: Throwable) {
                                if (e is MongoWriteException && ErrorCategory.fromErrorCode(e.code)
                                        == ErrorCategory.DUPLICATE_KEY) {
                                    claimCountDuplicate.incrementAndGet()
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

        info("Starting import ${ImportJob.options.sourceDir} into $ImportJob")
        val threads = List(ImportJob.options.parallel) {
            thread(name = "toMongoDB-import-$it") {
                while (!importing.isFinishing) {
                    val mFile: MFile? = importing.poll()
                    if (mFile != null) {
                        importing.start(mFile)
                        import(mFile.file)
                        importing.delete(mFile)
                    }
                }
            }
        }

        val done = importing.upload()
        threads.forEach(Thread::join)
        if (done) {
            fun ImportJob.options.ClaimType.createIndexes() {
                info("createIndexes for $this")
                if (importTo.isNotBlank()) {
                    (Document.parse(ImportJob::class.java
                            .getResourceAsStream("$type.createIndexes.json")
                            .use { it.reader().readText() })
                            ["indexes"] as? List<*>)
                            ?.forEach {
                                if (it is Document) {
                                    info("$importTo.createIndex(${it.toJson()})")
                                    importToCollection.createIndex(it)
                                }
                            }
                }
            }
            ImportJob.options.claimTypes.types
                    .map { ImportJob.options.claimTypes[it] }
                    .filter { it.createIndexes }
                    .forEach { it.createIndexes() }

            fun ImportJob.options.ClaimType.renameTo() {
                info("renameTo for $this")
                if (renameTo.isNotBlank()) {
                    if (renameToCollectionExists) {
                        renameToCollection.renameCollection(
                                MongoNamespace(ImportJob.data.name, renameTo
                                        + ".backup." + SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSSS").format(Date()))
                        )
                    }
                    importToCollection.renameCollection(MongoNamespace(ImportJob.data.name, renameTo))
                }
            }
            ImportJob.options.claimTypes.types
                    .map { ImportJob.options.claimTypes[it] }
                    .filter { it.renameTo.isNotEmpty() }
                    .forEach { it.renameTo() }

            info("DONE for ${ImportJob.options.sourceDir} into $ImportJob ${details()}")
        } else {
            warning("RESTARTING for ${ImportJob.options.sourceDir} into $ImportJob ${details()}")
            exitProcess(302)
        }
    } catch(e: Throwable) {
        error("ERROR: ${e.message} ${details()}", e)
        exitProcess(2)
    }
}

private data class MFile(val id: ObjectId?, val file: File)

private class MFiles {
    private val queue = LinkedBlockingQueue<MFile>(1)
    private val finishing = AtomicBoolean(false)
    private val restartMaxDuration = Duration.of(ImportJob.options.restartMaxDurationM, ChronoUnit.MINUTES)
    private val oneFileDuration = Duration.of(5, ChronoUnit.MINUTES)
    private val restartAt = Instant.now() + restartMaxDuration
    val isFinishing get() = finishing.get()

    fun upload(): Boolean {
        fun restartRequired(): Boolean {
            val memory = Runtime.getRuntime().let { it.maxMemory() - it.totalMemory() }

            if (memory < ImportJob.options.restartMemoryLimit) {
                warning("restartRequired: memory = $memory")
                return true
            }
            if (Instant.now().isAfter(restartAt)) {
                info("restartRequired: timeout $restartMaxDuration")
                return true
            }
            return false
        }

        var restartRequired = false
        while (sourceFiles.isNotEmpty()) {
            val mFile = sourceFiles.removeAt(0)
            trace("queue.put($mFile)")
            queue.put(mFile)
            if (restartRequired()) {
                restartRequired = true
                break
            }
        }
        if (restartRequired) {
            queue.clear()
        } else {
            val lastFileRestartOn = Instant.now() + oneFileDuration
            while (queue.isNotEmpty()) {
                Thread.sleep(1000)
                if (Instant.now().isAfter(lastFileRestartOn)) {
                    warning("restartRequired: last file timeout $oneFileDuration")
                    restartRequired = true
                }
            }
        }
        if (!restartRequired) {
            ImportJob.apiJobs.updateOne(ImportJob.jobFilter, doc { `+$set` { `+`("processing.finished", Date()) } })
        }
        finishing.set(true)
        return !restartRequired
    }

    fun start(file: MFile) {
        ImportJob.importFiles.updateOne(doc(file.id), doc { `+$set` { `+`("started", Date()) } })
    }

    fun delete(file: MFile) {
        ImportJob.importFiles.deleteOne(doc(file.id))
    }

    fun poll(): MFile? = queue.poll(5, TimeUnit.SECONDS)


    private val isStarting: Boolean by lazy {
        ImportJob.jobDoc?.let { job ->
            job["status"].let { status ->
                when (status) {
                    "NEW", "RUNNING" -> {
                        val processing = job["processing"] as? Document
                        if (processing == null) {
                            info("stating processing")
                            ImportJob.apiJobs.updateOne(ImportJob.jobFilter, doc {
                                `+$set` { `+`("processing", doc { `+`("started", Date()) }) }
                            })
                            val collectionNames = ImportJob.data.listCollectionNames()
                            ImportJob.options.claimTypes.types
                                    .map { ImportJob.options.claimTypes[it] }
                                    .filter { collectionNames.contains(it.importTo) }
                                    .forEach {
                                        warning("${it.importToCollection.namespace}.drop()")
                                        it.importToCollection.drop()
                                    }

                        } else {
                            info("continue processing")
                        }
                        processing == null
                    }
                    else -> throw AssertionError("Invalid job status $status for processing")
                }
            }
        } ?: throw AssertionError("apiJobs ${ImportJob.jobId} not found")
    }

    private val sourceFiles: MutableList<MFile> by lazy {
        mutableListOf<MFile>().also {
            fun File.walk(mode: String): List<File> {
                return when (mode) {
                    "R" -> walk().toList()
                    "D" -> listFiles()?.asList() ?: emptyList()
                    "F" -> listOf(this)
                    else -> emptyList()
                }.filter {
                    it.isFile
                }
            }
            if (isStarting) {
                with(ImportJob.options) {
                    info("scanning $sourceDir in $scanMode mode")
                    sourceDir.walk(scanMode).forEach { file ->
                        val fileDoc = doc {
                            `+`("job", ImportJob.jobId)
                            `+`("file", file.absolutePath)
                        }
                        ImportJob.importFiles.insertOne(fileDoc)
                        it += MFile(fileDoc._id as? ObjectId, file)
                    }
                    info("scanning done")
                }
            } else {
                info("loading importFiles")
                ImportJob.importFiles.find(doc { `+`("job", ImportJob.jobId) }).forEach { document ->
                    it += MFile(document._id as? ObjectId, File(document.getString("file")))
                }
                info("loading done")
            }
        }
    }
}

private sealed class LogLevel(val value: Int) {
    override fun toString(): String = javaClass.simpleName
}

private object TRACE : LogLevel(100)
private object INFO : LogLevel(500)
private object WARNING : LogLevel(1000)
private object ERROR : LogLevel(5000)

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
        """${level.toString().padEnd(7)} $message at $now log: ObjectId("$_id")""".also {
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
        val document = doc {
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
        }
        ImportJob.apiJobsLog.insertOne(document)
        printLog(message, document._id)
    } catch(e: Throwable) {
        e.printStackTrace()
        printLog("${e.javaClass.simpleName} on $message", null)
        if (e !is BsonSerializationException) {
            exit("Logging error: ${e.message}")
        }
    }
}

private fun trace(message: String, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
        level = TRACE,
        message = message,
        details = details,
        detailsJson = detailsJson,
        detailsXml = detailsXml
)

private fun info(message: String, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
        level = INFO,
        message = message,
        details = details,
        detailsJson = detailsJson,
        detailsXml = detailsXml
)

private fun warning(message: String, error: Throwable? = null, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
        level = WARNING,
        message = message,
        error = error,
        details = details,
        detailsJson = detailsJson,
        detailsXml = detailsXml
)

private fun error(message: String, error: Throwable? = null, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) = log(
        level = ERROR,
        message = message,
        error = error,
        details = details,
        detailsJson = detailsJson,
        detailsXml = detailsXml
)
