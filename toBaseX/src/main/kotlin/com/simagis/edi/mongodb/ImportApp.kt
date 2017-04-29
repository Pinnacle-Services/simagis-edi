package com.simagis.edi.mongodb

import com.berryworks.edireader.EDISyntaxException
import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.simagis.edi.basex.ISA
import com.simagis.edi.mdb.*
import org.basex.core.Context
import org.basex.core.MainOptions
import org.basex.core.cmd.CreateDB
import org.basex.core.cmd.Replace
import org.basex.core.cmd.XQuery
import org.bson.Document
import org.bson.types.ObjectId
import java.io.File
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
import javax.json.Json
import javax.json.JsonArray
import javax.json.JsonObject
import kotlin.concurrent.thread
import kotlin.system.exitProcess


fun main(args: Array<String>) {
    ImportJob.open(args)
    info("starting job", detailsJson = ImportJob.jobDoc)
    val importing = MFiles()

    val xqTypes: MutableMap<String, String> = ConcurrentHashMap()

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

        fun import(mFile: MFile) {
            val isaList: List<ISA>
            try {
                isaList = ISA.read(mFile.file)
                fileCount.incrementAndGet()
            } catch(e: Exception) {
                fileCountInvalid.incrementAndGet()
                warning("Invalid file $mFile", e)
                importing.fileStatusUpdate(mFile, e)
                return
            }

            isaList.forEach { isa ->
                val mISA = MISA(mFile, isa)
                if (importing.isaStatus(mISA) == "NEW") {
                    val claims = isa.toClaimsJsonArray(mFile.file)
                    if (claims != null) {
                        isaCount.incrementAndGet()
                        claims.forEach { json ->
                            fun ImportJob.options.ClaimType.insert(archiveMode: Boolean) {
                                if (name.isBlank()) return
                                val document = Document.parse(json.toString()).prepare()
                                try {
                                    fun Document.inRange(start: Date?) = archiveMode || when (isa.type) {
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
                            if (json is JsonObject) {
                                ImportJob.options.claimTypes[isa.type].insert(false)
                                ImportJob.options.archive["${isa.type}a"].insert(true)
                            }
                        }
                        importing.isaStatusUpdate(mISA, "ARCHIVED")
                    } else {
                        isaCountInvalid.incrementAndGet()
                        importing.isaStatusUpdate(mISA, AssertionError(
                                "isa.toClaimsJsonArray(mFile.file) == null"))
                    }
                }
            }

            importing.fileStatusUpdate(mFile, "ARCHIVED")
        }

        info("Starting import ${ImportJob.options.sourceDir} into $ImportJob")
        val threads = List(ImportJob.options.parallel) {
            thread(name = "toMongoDB-import-$it") {
                while (!importing.isFinishing) {
                    val mFile: MFile? = importing.poll()
                    if (mFile != null) {
                        importing.start(mFile)
                        import(mFile)
                        importing.delete(mFile)
                    }
                }
            }
        }

        val done = importing.upload()
        threads.forEach(Thread::join)
        if (done) {
            val claimTypes: List<ImportJob.options.ClaimType> = ImportJob.options.archive.types
                    .map { ImportJob.options.archive[it] } + ImportJob.options.claimTypes.types
                    .map { ImportJob.options.claimTypes[it] }

            claimTypes
                    .filter { it.createIndexes }
                    .forEach { it.createIndexes() }

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

private fun md(): MessageDigest = MessageDigest.getInstance("SHA")

private fun ByteArray.toHexString(): String = joinToString(separator = "") {
    Integer.toHexString(it.toInt() and 0xff)
}

private fun File.digest() = inputStream().use { stream ->
    md().apply {
        val bytes = ByteArray(4096)
        while (true) {
            val len = stream.read(bytes)
            if (len == -1) break
            update(bytes, 0, len)
        }
    }.digest().toHexString()
}


private data class MISA(val mFile: MFile, val isa: ISA, var digest: String = "")

private data class MFile(val id: ObjectId?, val file: File, var digest: String = "")

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

        var count = sourceFiles.count()
        ImportJob.updateProcessing("filesLeft", count)
        var restartRequired = false
        while (sourceFiles.isNotEmpty()) {
            val mFile = sourceFiles.removeAt(0)
            trace("queue.put($mFile)")
            if (--count % 10 == 0) {
                ImportJob.updateProcessing("filesLeft", count)
            }
            if (fileStatus(mFile) == "NEW") {
                queue.put(mFile)
            }
            if (restartRequired()) {
                restartRequired = true
                break
            }
        }
        ImportJob.updateProcessing("filesLeft", count)
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

    fun fileStatus(mFile: MFile): String? {
        mFile.digest = mFile.file.digest()
        val filter = doc(mFile.digest)
        ImportJob.digest.file.find(filter).first().let { found: Document? ->
            if (found == null) {
                ImportJob.digest.file.insertOne(doc {
                    `+`("_id", mFile.digest)
                    `+`("names", listOf(mFile.file.name))
                    `+`("status", "NEW")
                })
                return "NEW"
            } else {
                ImportJob.digest.file.updateOne(filter, doc {
                    `+$addToSet` { `+`("names", mFile.file.name) }
                })
                return found["status"] as? String
            }
        }
    }

    fun fileStatusUpdate(mFile: MFile, value: String) {
        ImportJob.digest.file.updateOne(doc(mFile.digest), doc {
            `+$set` { `+`("status", value) }
        })
    }

    fun fileStatusUpdate(mFile: MFile, error: Throwable) {
        ImportJob.digest.file.updateOne(doc(mFile.digest), doc {
            `+$set` {
                `+`("status", "ERROR")
                appendError(error)
            }
        })
    }

    fun isaStatus(mISA: MISA): String? {
        val byteArray = mISA.isa.code.toByteArray(ISA.CHARSET)
        mISA.digest = md().digest(byteArray).toHexString()

        val filter = doc(mISA.digest)
        ImportJob.digest.isa.find(filter).first().let { found: Document? ->
            if (found == null) {
                ImportJob.digest.isa.insertOne(doc {
                    `+`("_id", mISA.digest)
                    `+`("files", listOf(mISA.mFile.digest))
                    `+`("status", "NEW")
                })
                return "NEW"
            } else {
                ImportJob.digest.isa.updateOne(filter, doc {
                    `+$addToSet` { `+`("files", mISA.mFile.digest) }
                })
                return found["status"] as? String
            }
        }
    }

    fun isaStatusUpdate(mISA: MISA, value: String) {
        ImportJob.digest.isa.updateOne(doc(mISA.digest), doc {
            `+$set` { `+`("status", value) }
        })
    }

    fun isaStatusUpdate(mISA: MISA, error: Throwable) {
        ImportJob.digest.isa.updateOne(doc(mISA.digest), doc {
            `+$set` {
                `+`("status", "ERROR")
                appendError(error)
            }
        })
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
                    ImportJob.importFiles.drop()
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