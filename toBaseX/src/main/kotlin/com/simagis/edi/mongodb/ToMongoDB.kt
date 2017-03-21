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
fun main(args: Array<String>) {
    val commandLine = com.berryworks.edireader.util.CommandLine(args)
    val restartable: String? = commandLine["restartable"]
    val host = commandLine["host"] ?: "localhost"
    val xq = commandLine["xq"] ?: File("isa-claims-xq").absolutePath
    val db = commandLine["db"] ?: "claims"
    val mode = commandLine["mode"] ?: "R"
    val parallel = commandLine["parallel"]?.toInt() ?: Runtime.getRuntime().availableProcessors()
    val after = commandLine["after"]?.let { java.sql.Date.valueOf(it) }
    val insert = (commandLine["insert"] ?: "true") == "true"

    if (commandLine.size() == 0)
        exit("""
        Usage: ToMongoDB.kt [-host host] [-xq path] [-db database] [-mode R|D|F] [-restartable sessionId] [-parallel n] <path>
            host: $host
            xq: $xq
            database: $db
            mode: $mode
            parallel: $parallel""")

    val mongoClient = MongoClient(host)
    val database: MongoDatabase = mongoClient.getDatabase(db)
    val path = File(commandLine[0])

    val importing = MFiles(restartable, path, mode, database)

    val xqDir = File(xq)
    val xqTypes: MutableMap<String, String> = Collections.synchronizedMap(mutableMapOf<String, String>())

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

    class MongoClaims {
        val collectionNameFormat = System.getProperty("claims.collection.nameFormat", "claims_%s")
        val log: MongoCollection<Document> = database.getCollection("claimsLog")
        operator fun get(type: String): MongoCollection<Document> = claimTypeMap.getOrPut(type) {
            database.getCollection(collectionNameFormat.format(type))
        }

        private var claimTypeMap: MutableMap<String, MongoCollection<Document>> = mutableMapOf()
    }

    val mongoClaims: ThreadLocal<MongoClaims> = ThreadLocal.withInitial { MongoClaims() }

    val printLogLock = ReentrantLock()
    fun log(level: String, message: String, error: Throwable? = null, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) {
        val now = Date()
        fun printLog(message: String, _id: Any?) {
            """${level.padEnd(7)} $message at $now log: ObjectId("$_id}")""".also {
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
            mongoClaims.get().log.insertOne(document)
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

            val xqText = stat.doc.type?.let { type ->
                xqTypes.getOrPut(type) {
                    xqDir.resolve("isa-claims-$type.xq").let { file ->
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
        } catch(e: Exception) {
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
                        fun append(value: String?, cast: (String) -> Any) {
                            if (value != null && value.isNotBlank())
                                append(key.name(), cast(value))
                        }
                        when (key.type()) {
                            "I" -> append(getString(key), String::toInt)
                            "L" -> append(getString(key), String::toLong)
                            "F" -> append(getString(key), String::toDouble)
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
                val claims = isa.toClaimsJsonArray()
                if (claims != null) {
                    isaCount.incrementAndGet()
                    claims.forEach {
                        if (it is JsonObject) {
                            val collection = mongoClaims.get()[isa.type]
                            val document = Document.parse(it.toString()).prepare()
                            try {
                                fun Document.inRange(start: Date) = when (isa.type) {
                                    "835" -> getDate("procDate")?.after(start) ?: false
                                    "837" -> getDate("sendDate")?.after(start) ?: false
                                    else -> true
                                }

                                if (after == null || document.inRange(after)) {
                                    if (insert) {
                                        collection.insertOne(document)
                                    }
                                    claimCount.incrementAndGet()
                                }
                            } catch(e: Throwable) {
                                if (e is MongoWriteException && ErrorCategory.fromErrorCode(e.code)
                                        == ErrorCategory.DUPLICATE_KEY) {
                                    claimCountDuplicate.incrementAndGet()
// TODO: compare document by content or digest
//                                    val old: Document? = collection.find(Document("_id", document["_id"])).first()
//                                    if (old != document) {
//                                        warning("DUPLICATE", detailsJson = document)
//                                    }
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

        info("Starting import $path into $host $db")
        val threads = List(parallel) {
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
        info("DONE for $path into $host $db ${details()}")
        if (!done) {
            warning("RESTARTING")
            exitProcess(302)
        }
    } catch(e: Throwable) {
        error("ERROR: ${e.message} ${details()}", e)
        exitProcess(2)
    }
}

private data class MFile(val id: ObjectId, val file: File)

private class MFiles(
        private val session: String?,
        private val path: File,
        private val mode: String,
        private val database: MongoDatabase
) {
    private val queue = LinkedBlockingQueue<MFile>(1)
    private val finishing = AtomicBoolean(false)
    private val maxDuration = Duration.of(1, ChronoUnit.HOURS)
    private val oneFileDuration = Duration.of(5, ChronoUnit.MINUTES)
    private val restartAt = Instant.now() + maxDuration
    val isFinishing get() = finishing.get()

    fun upload(): Boolean {
        fun restartRequired(): Boolean {
            val memory = Runtime.getRuntime().let { it.maxMemory() - it.totalMemory() }
            if (memory < 500 * 1024 * 1024) {
                println("restartRequired: memory = $memory")
                return true
            }
            if (Instant.now().isAfter(restartAt)) {
                println("restartRequired: timeout $maxDuration")
                return true
            }
            return false
        }

        var restartRequired = false
        while (sourceFiles.isNotEmpty()) {
            val mFile = sourceFiles.removeAt(0)
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
                    println("restartRequired: last file timeout $oneFileDuration")
                    restartRequired = true
                }
            }
        }
        if (!restartRequired) {
            session?.let { importSessions.deleteOne(Document("_id", session)) }
        }
        finishing.set(true)
        return !restartRequired
    }

    fun start(file: MFile) {
        importFiles?.updateOne(Document("_id", file.id), Document('$' + "set", Document("started", Date())))
    }

    fun delete(file: MFile) {
        importFiles?.deleteOne(Document("_id", file.id))
    }

    fun poll(): MFile? = queue.poll(5, TimeUnit.SECONDS)

    private val importSessions = database.getCollection("importSessions")

    private val isSessionNew: Boolean = session?.let {
        importSessions.find(Document("_id", session)).let {
            if (it.first() == null) {
                importSessions.insertOne(Document("_id", session).append("started", Date()))
                true
            } else false
        }
    } ?: true

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
            if (isSessionNew) {
                path.walk(mode).forEach { file ->
                    val document = Document("session", session).append("file", file.absolutePath)
                    importFiles?.insertOne(document)
                    it += MFile(document.getObjectId("_id"), file)
                }
            } else {
                importFiles?.find(Document("session", session))?.forEach { document ->
                    it += MFile(document.getObjectId("_id"), File(document.getString("file")))
                }
            }
        }
    }

    private val importFiles: MongoCollection<Document>? = session?.let {
        database.getCollection("importFiles")
    }
}
