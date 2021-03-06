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


    fun ISA.toClaimsJsonArray(file: File, onError: (Throwable?) -> Unit = {}): JsonArray? {
        fun invalidISA(isa: ISA, e: Throwable? = null) {
            onError(e)
            warning(
                    "Invalid ISA: ${isa.stat} from $file",
                    e,
                    details = isa.code,
                    detailsXml = if (e !is EDISyntaxException) try {
                        isa.toXML().toString(ISA.CHARSET)
                    } catch (e: Exception) {
                        null
                    } else null
            )
        }

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
        } catch (e: Throwable) {
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
    val claimCountReplaced = AtomicLong()
    fun details() = """
        DETAILS:
            files: ${fileCount.get()}
            files duplicate: ${fileCountDuplicate.get()}
            files invalid: ${fileCountInvalid.get()}
            ISAs: ${isaCount.get()}
            ISAs invalid: ${isaCountInvalid.get()}
            claims: ${claimCount.get()}
            claims duplicate: ${claimCountDuplicate.get()}
            claims replaced: ${claimCountReplaced.get()}
            claims invalid: ${claimCountInvalid.get()}
"""
    try {
        fun Document.prepare(logger: LocalLogger): Document {
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
                                parseDT8(value)?.also { append(key.name(), it) } ?: if (value.isNotEmpty()) {
                                    logger.warn("Invalid DT8 value at $_id $key: '$value'")
                                }
                            }
                        }
                    } catch (e: Exception) {
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
            when (logger.isa.type) {
                "835" -> augment835()
                "837" -> augment837()
            }
            return this
        }

        data class DuplicatesKey(val claimType: String, val claimId: String)
        val duplicates = mutableMapOf<DuplicatesKey, Date>()
        val duplicatesLock = ReentrantLock()

        fun Document.claimDate(isa: ISA): Date? = when (isa.type) {
            "835" -> getDate("procDate")
            "837" -> getDate("sendDate")
            else -> null
        }

        fun ImportJob.options.ClaimType.key(claimId: String): DuplicatesKey = DuplicatesKey(type, claimId)

        fun ImportJob.options.ClaimType.tryReplace(collection: DocumentCollection, newClaimDocument: Document, newClaimDate: Date, isa: ISA, logger: LocalLogger) {
            claimCountDuplicate.incrementAndGet()
            val claimId = newClaimDocument._id.toString()
            val oldClaimDate = collection.find(doc(claimId)).first().claimDate(isa)!!
            return if (oldClaimDate < newClaimDate) {
                collection.findOneAndReplace(doc(claimId), newClaimDocument)
                claimCountReplaced.incrementAndGet()
                logger.trace("duplicate $claimId: old = $oldClaimDate; new = $newClaimDate -> REPLACED")
                duplicates[key(claimId)] = newClaimDate
            } else {
                logger.trace("duplicate $claimId: old = $oldClaimDate; new = $newClaimDate -> IGNORED")
            }
        }

        fun import(file: File) {
            if (!digests.add(file.digest())) {
                fileCountDuplicate.incrementAndGet()
                info("Duplicate file $file")
                return
            }

            val acnLog = ImportJob.acn_log.db.files[file.name].toAcnLog()
            try {
                val isaList: List<ISA>
                try {
                    isaList = ISA.read(file)
                    fileCount.incrementAndGet()
                } catch (e: Throwable) {
                    fileCountInvalid.incrementAndGet()
                    warning("Invalid file $file", e)
                    acnLog.onFileError(e)
                    return
                }
                isaList.forEach { isa ->
                    val claims = isa.toClaimsJsonArray(file, onError = { acnLog.onIsaError(it, isa) })
                    if (claims != null) {
                        isaCount.incrementAndGet()
                        claims.forEach { json ->
                            fun ImportJob.options.ClaimType.insert(archiveMode: Boolean) {
                                if (temp.isBlank()) return
                                val logger = LocalLogger(file, isa, json)
                                val collection = tempCollection
                                val document = Document.parse(json.toString()).prepare(logger)
                                try {
                                    fun Date?.inRange(start: Date?) = archiveMode || when (isa.type) {
                                        "835" -> this?.after(start) ?: false
                                        "837" -> this?.after(start) ?: false
                                        else -> false
                                    }

                                    val claimDate = document.claimDate(isa) ?: throw AssertionError("Invalid claim date")
                                    if (claimDate.inRange(ImportJob.options.after)) {
                                        document["file"] = file.name
                                        val claimId = document._id.toString()
                                        val insertMode: Boolean = duplicatesLock.withLock {
                                            val duplicatesKey = key(claimId)
                                            val oldClaimDate = duplicates[duplicatesKey]
                                            when {
                                                oldClaimDate == null -> {
                                                    duplicates[duplicatesKey] = claimDate
                                                    true
                                                }
                                                oldClaimDate >= claimDate -> {
                                                    logger.trace("duplicate $claimId: old = $oldClaimDate; new = $claimDate -> IGNORED LOCALLY")
                                                    false
                                                }
                                                oldClaimDate < claimDate -> {
                                                    tryReplace(collection, document, claimDate, isa, logger)
                                                    false
                                                }
                                                else -> throw AssertionError("Invalid claim date logic")
                                            }
                                        }

                                        if (insertMode) try {
                                            collection.insertOne(document)
                                        } catch (e: MongoWriteException) {
                                            if (ErrorCategory.fromErrorCode(e.code) != ErrorCategory.DUPLICATE_KEY)
                                                throw e
                                            duplicatesLock.withLock { tryReplace(collection, document, claimDate, isa, logger) }
                                        }
                                        claimCount.incrementAndGet()
                                        acnLog.onClaimInserted(isa, document)
                                    } else {
                                        acnLog.onClaimOutOfRange(isa, document)
                                    }
                                } catch (e: Throwable) {
                                    acnLog.onClaimError(e, isa, document)
                                    claimCountInvalid.incrementAndGet()
                                    logger.warn("insertOne error: ${e.message}", e)
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
            } finally {
                acnLog.onFileProcessed()
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
    } catch (e: Throwable) {
        error("ERROR: ${e.message} ${details()}", e)
        exitProcess(2)
    }
}

fun parseDT8(text: String): Date? {
    val iso = when (text.length) {
        4 -> "$text-01-01T12:00:00.000Z"
        6 -> "${text.substring(0, 4)}-${text.substring(4, 6)}-01T12:00:00.000Z"
        8 -> "${text.substring(0, 4)}-${text.substring(4, 6)}-${text.substring(6, 8)}T12:00:00.000Z"
        else -> return null
    }
    return Date.from(Instant.parse(iso))
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

private fun ImportJob.acn_log.file?.toAcnLog(): AcnLog = if (this != null) AcnLogImpl(this) else object : AcnLog {}

private interface AcnLog {
    fun onFileError(e: Throwable) {}
    fun onIsaError(e: Throwable?, isa: ISA) {}
    fun onClaimError(e: Throwable, isa: ISA, document: Document) {}
    fun onClaimOutOfRange(isa: ISA, document: Document) {}
    fun onClaimInserted(isa: ISA, document: Document) {}
    fun onFileProcessed() {}
}

private class AcnLogImpl(val file: ImportJob.acn_log.file) : AcnLog {
    private val claimsProcessed = mutableListOf<String>()
    private val claimsInserted = mutableListOf<String>()

    override fun onFileError(e: Throwable) {
    }

    override fun onIsaError(e: Throwable?, isa: ISA) {
    }

    override fun onClaimError(e: Throwable, isa: ISA, document: Document) {
    }

    override fun onClaimOutOfRange(isa: ISA, document: Document) {
        claimsProcessed += document._id as String
    }

    override fun onClaimInserted(isa: ISA, document: Document) {
        val id = document._id as String
        claimsProcessed += id
        claimsInserted += id
    }

    override fun onFileProcessed() {
        claimsProcessed.clear()
        claimsInserted.clear()
    }
}

private class LocalLogger(val file: File, val isa: ISA, val json: Any?) {
    fun warn(message: String, error: Throwable? = null, details: String? = null, detailsJson: Any? = null, detailsXml: String? = null) {
        warning(message + " file: $file", error, details, detailsJson ?: json, detailsXml ?: isa.toXmlCode())
    }

    fun trace(message: String) {
        trace(message + " file: ${file.name}", details = null)
    }
}

private data class MFile(val id: ObjectId?, val file: File, val fileTime: Long)

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
        mutableListOf<MFile>().also { list ->
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
                        val fileTime = file.lastModified()
                        val fileDoc = doc {
                            `+`("job", ImportJob.jobId)
                            `+`("file", file.absolutePath)
                            `+`("fileTime", fileTime)
                        }
                        ImportJob.importFiles.insertOne(fileDoc)
                        list += MFile(fileDoc._id as? ObjectId, file, fileTime)
                    }
                    info("scanning done")
                }
            } else {
                info("loading importFiles")

                ImportJob.importFiles
                        .find(doc { `+`("job", ImportJob.jobId) })
                        .forEach { document ->
                            list += MFile(
                                    id = document._id as? ObjectId,
                                    file = File(document.getString("file")),
                                    fileTime = document.getLong("fileTime"))
                        }
                info("loading done")
            }
            list.sortByDescending { it.fileTime }
        }
    }
}