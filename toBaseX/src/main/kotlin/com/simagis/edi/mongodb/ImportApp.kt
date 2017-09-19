package com.simagis.edi.mongodb

import com.berryworks.edireader.EDISyntaxException
import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.simagis.edi.basex.ISA
import com.simagis.edi.mdb._id
import com.simagis.edi.mdb.`+$set`
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
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
                    ImportJob.options.xqDir.resolve("isa-claims-$type.xq").let { xqFile ->
                        if (xqFile.isFile) xqFile.readText() else {
                            warning("$xqFile not found")
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
        fun Document.prepare(logger: LocalLogger): Document {
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
                                        logger.warn("Invalid DT8 value at $_id $key: '${getString(key)}'")
                                    }
                                }
                            }
                        }
                    } catch(e: Exception) {
                        when (e) {
                            is ParseException -> logger.warn("Invalid date format at $_id $key: '${getString(key)}'")
                            is NumberFormatException -> logger.warn("Invalid number format at $_id $key: '${getString(key)}'")
                            else -> throw e
                        }
                    }
                }
                keys.removeIf(String::isTyped)
                values.forEach {
                    when (it) {
                        is Document -> it.fixTypes()
                        is List<*> -> it.forEach { (it as? Document)?.fixTypes() }
                    }
                }
            }
            fixTypes()
            when(logger.isa.type) {
                "835" -> augment835()
                "837" -> augment837()
            }
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
                    claims.forEach { json ->
                        fun ImportJob.options.ClaimType.insert(archiveMode: Boolean) {
                            if (temp.isBlank()) return
                            val logger = LocalLogger(file, isa, json)
                            val collection = tempCollection
                            val document = Document.parse(json.toString()).prepare(logger)
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
                                    logger.warn("insertOne error: ${e.message}", e)
                                }

                            }
                        }
                        if (json is JsonObject) {
                            ImportJob.options.claimTypes[isa.type].insert(false)
                            ImportJob.options.archive["${isa.type}a"].insert(true)
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
            val claimTypes: List<ImportJob.options.ClaimType> = ImportJob.options.archive.types
                    .map { ImportJob.options.archive[it] } + ImportJob.options.claimTypes.types
                    .map { ImportJob.options.claimTypes[it] }

            claimTypes
                    .filter { it.createIndexes }
                    .forEach { it.createIndexes() }

            claimTypes
                    .filter { it.target.isNotEmpty() }
                    .forEach { it.renameToTarget() }

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

fun Document.augment835() {
    // https://github.com/vylegzhanin/simagis-edi/issues/2
    (this["svc"] as? List<*>)?.forEach {
        (it as? Document)?.let { cpt ->
            var cptPr = 0.0
            (cpt["adj"] as? List<*>)?.forEach {
                (it as? Document)?.let { adj ->
                    if (adj["adjGrp"] == "PR") {
                        (adj["adjAmt"] as? Number)?.let { adjAmt ->
                            cptPr += adjAmt.toDouble()
                        }
                    }
                }
            }
            val cptPay = (cpt["cptPay"] as? Number)?.toDouble() ?: 0.0
            cpt["cptPr"] = cptPr
            cpt["cptAll"] = cptPr + cptPay
        }
    }
}

fun Document.augment837() {
    val acn = this["acn"] as? String
    if (acn != null) {
        ImportJob.billed_acn[acn]?.let {
            this["prid"] = it.prid
        }
    }
}

private class LocalLogger(val file: File, val isa: ISA, val json: Any?) {
    fun warn(message: String, error: Throwable? = null, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) {
        warning(message + " file: $file", error, details, detailsJson ?: json, detailsXml ?: isa.toXmlCode())
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

        var count = sourceFiles.count()
        ImportJob.updateProcessing("filesLeft", count)
        var restartRequired = false
        while (sourceFiles.isNotEmpty()) {
            val mFile = sourceFiles.removeAt(0)
            trace("queue.put($mFile)")
            if (--count % 10 == 0) {
                ImportJob.updateProcessing("filesLeft", count)
            }
            queue.put(mFile)
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
                            val collectionNames = ImportJob.claims.listCollectionNames()
                            ImportJob.options.claimTypes.types
                                    .map { ImportJob.options.claimTypes[it] }
                                    .filter { collectionNames.contains(it.temp) }
                                    .forEach {
                                        warning("temp collection already exists -> ${it.tempCollection.namespace}.drop()")
                                        it.tempCollection.drop()
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