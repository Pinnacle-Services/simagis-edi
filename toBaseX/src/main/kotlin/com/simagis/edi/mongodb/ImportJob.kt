package com.simagis.edi.mongodb

import com.mongodb.MongoNamespace
import com.mongodb.client.MongoDatabase
import org.bson.Document
import java.io.*
import java.net.URI
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.security.MessageDigest
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import kotlin.concurrent.withLock


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/9/2017.
 */

internal object ImportJob : AbstractJob() {
    val importFiles: DocumentCollection by lazy { claimsAPI.getCollection("importFiles") }

    object options {
        private val options: Document by lazy { jobDoc?.get("options") as? Document ?: Document() }
        val sourceDir: File by lazy { (options["sourceDir"] as? String)?.let(::File) ?: File("/DATA/source_files") }
        val scanMode: String by lazy { (options["scanMode"] as? String) ?: "R" }
        val xqDir: File by lazy { (options["xqDir"] as? String)?.let(::File) ?: File("isa-claims-xq") }
        val parallel: Int by lazy { (options["parallel"] as? Int) ?: Runtime.getRuntime().availableProcessors() }
        val after: java.util.Date?  by lazy {
            options["after"]?.let {
                when (it) {
                    is java.util.Date -> it
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
                ClaimType.of(type, (claimTypes[type] as? Document) ?: Document())
            }
        }

        object archive {
            private val archive: Document by lazy { options["archive"] as? Document ?: Document() }
            private val cache: MutableMap<String, ClaimType> = ConcurrentHashMap()

            val types: Set<String> get() = archive.keys

            operator fun get(type: String): ClaimType = cache.computeIfAbsent(type) {
                ClaimType.of(type, (archive[type] as? Document) ?: Document(), db = claimsA)
            }
        }

        object build835c {
            private val build835c: Document by lazy { options["build835c"] as? Document ?: Document() }
            val _835c: ClaimType by lazy {
                ClaimType.of("835c", build835c["835c"] as? Document ?: Document())
            }
            val clients: DocumentCollection by lazy {
                claims.getCollection(build835c["clients"] as? String ?: "clientid")
            }
        }

        object build835ac {
            private val build835ac: Document by lazy { options["build835ac"] as? Document ?: Document() }
            val _835ac: ClaimType by lazy {
                ClaimType.of("835ac", build835ac["835ac"] as? Document ?: Document(), claimsA)
            }
            val clients: DocumentCollection by lazy {
                claims.getCollection(build835ac["clients"] as? String ?: "clientid")
            }
        }

        data class ClaimType(
                val type: String,
                val temp: String,
                val target: String,
                val createIndexes: Boolean,
                val db: MongoDatabase) {
            val tempCollection: DocumentCollection by lazy { db.getCollection(temp) }
            val targetCollection: DocumentCollection by lazy { db.getCollection(target) }

            companion object {
                internal fun of(type: String, claimType: Document, db: MongoDatabase = claims) = ClaimType(
                        type = type,
                        temp = claimType["temp"] as? String ?: "claims_$type.temp",
                        target = claimType["target"] as? String ?: "claims_$type.target",
                        createIndexes = claimType["createIndexes"] as? Boolean ?: true,
                        db = db
                )
            }
        }

    }

    object billed_acn {
        class doc(val id: String, val prid: String, val prg: String)

        val map: Map<String, doc> by lazy {
            mutableMapOf<String, doc>().apply {
                var id: Int = -1
                var prid: Int = -1
                var prg: Int = -1
                File("files")
                        .resolve("xifin_billed_acn.gzip")
                        .parseAsGzCsv(
                                { index, name ->
                                    when (name) {
                                        "id" -> id = index
                                        "prid" -> prid = index
                                        "prg" -> prg = index
                                    }
                                },
                                { record ->
                                    doc(
                                            id = record[id],
                                            prid = record[prid],
                                            prg = record[prg]
                                    ).also {
                                        this[it.id] = it
                                    }
                                })
            }
        }

        operator fun get(acn: String): doc? = acn_to_id(acn)?.let { map[it] }

        private val ZDD = "Z\\d{2}$".toRegex()
        internal fun acn_to_id(acn: String): String? = when {
            acn.length > 3 && ZDD.containsMatchIn(acn) -> acn.replace(ZDD, "")
            else -> null
        }
    }

    object acn_log {
        interface file {
            val name: String
            val accnSet: Set<accn>
        }

        interface accn {
            val id: String
            val payor: String
            val file: file
        }

        interface DB {
            val files: Map<String, file>
            val accns: Map<String, accn>
        }

        val db: DB by lazy {
            class fileImpl(override val name: String, override val accnSet: MutableSet<accn> = mutableSetOf()) : file
            class accnImpl(override val id: String, override val payor: String, override val file: fileImpl) : accn

            val files = mutableMapOf<String, fileImpl>()
            val accns = mutableMapOf<String, accn>()

            var ACCN_ID = -1
            var PAYOR_ID = -1
            var FILENAME = -1
            File("files")
                    .resolve("xifin_acn_log.gzip")
                    .parseAsGzCsv(
                            { index, name ->
                                when (name) {
                                    "ACCN_ID" -> ACCN_ID = index
                                    "PAYOR_ID" -> PAYOR_ID = index
                                    "FILENAME" -> FILENAME = index
                                }
                            },
                            { record ->
                                val accnId = record[ACCN_ID]
                                val payorId = record[PAYOR_ID]
                                val fileName = record[FILENAME]
                                val file = files.getOrPut(fileName) { fileImpl(fileName) }
                                val accn = accns.getOrPut(accnId) { accnImpl(accnId, payorId, file) }
                                file.accnSet += accn
                            })

            object : DB {
                override val files: Map<String, file> = files
                override val accns: Map<String, accn> = accns
            }
        }

        interface FS {
            fun generalize(fileName: String): String = fileName.generalizeFileName()
        }

        val fs: FS by lazy {
            val src = File("files").resolve("xifin_acn_log.gzip")
            if (src.isFile) {
                val md5 = src.md5()
                val acnByFile = src.parentFile.resolve("${src.name}.$md5.acnByFile.zip")
                val acnToPrid = src.parentFile.resolve("${src.name}.$md5.acnToPrid.gzip")
                if (!(acnByFile.isFile && acnToPrid.isFile)) {
                    acnByFile.delete()
                    acnToPrid.delete()
                    fun File.newZipFs(): FileSystem = FileSystems
                            .newFileSystem(URI.create("jar:" + toURI()), mapOf("create" to "true"))

                    val acnToPridFs = acnToPrid.newZipFs()

                    class NamedBuffer(
                            val name: String,
                            private val array: ByteArrayOutputStream = ByteArrayOutputStream()) : BufferedWriter(OutputStreamWriter(array)) {
                        val byteArray: ByteArray get() {
                            flush()
                            return array.toByteArray()
                        }
                    }

                    val closeables = mutableListOf<Closeable>()
                    try {
                        fun <T : Closeable> T.autoClose(): T = apply { closeables += this }
                        val acnByFileWriters = mutableMapOf<String, NamedBuffer>()
                        val prid2acns = mutableMapOf<String, MutableSet<String>>()
                        var ACCN_ID = -1
                        var PAYOR_ID = -1
                        var FILENAME = -1
                        src.parseAsGzCsv(
                                { index, name ->
                                    when (name) {
                                        "ACCN_ID" -> ACCN_ID = index
                                        "PAYOR_ID" -> PAYOR_ID = index
                                        "FILENAME" -> FILENAME = index
                                    }
                                },
                                { record ->
                                    val accnId = record[ACCN_ID]
                                    val payorId = record[PAYOR_ID]
                                    val fileName = record[FILENAME].generalizeFileName()
                                    acnByFileWriters
                                            .getOrPut(fileName) { NamedBuffer(fileName).autoClose() }
                                            .append("$accnId\n")
                                    prid2acns.getOrPut(payorId) { mutableSetOf() } += accnId
                                })

                        acnByFile.newZipFs().use { fs ->
                            acnByFileWriters.values.forEach { buffer ->
                                Files.newOutputStream(fs.getPath(buffer.name)).writer().use { writer ->
                                    buffer.byteArray.inputStream().reader().readLines().toSortedSet().forEach {
                                        writer.append("$it\n")
                                    }
                                }
                            }
                        }

                        GZIPOutputStream(FileOutputStream(acnToPrid)).bufferedWriter().use { csv ->
                            csv.append("acn\tprid\n")
                            prid2acns.forEach { prid, acns ->
                                acns.forEach { acn ->
                                    csv.append("$acn\t$prid\n")
                                }
                            }
                        }

                    } finally {
                        closeables.forEach {
                            it.close()
                        }
                        acnToPridFs.close()
                    }
                }
            }
            object : FS {}
        }

        val log: DocumentCollection by lazy {
            claims.getCollection("acn_log_" + jobId)
        }
    }
}


private fun String.generalizeFileName(): String = toLowerCase()
        .removeSuffix(".gz")
        .removeSuffix(".txt")

private fun File.md5() = MessageDigest.getInstance("MD5").let { md5 ->
    fun ByteArray.toHexString(): String = joinToString(separator = "") {
        Integer.toHexString(it.toInt() and 0xff).let {
            if (it.length == 2) it else "0$it"
        }
    }
    md5.update(inputStream()
            .use { stream -> ByteArray(length().toInt()).also { stream.read(it) } })
    md5.digest().toHexString()
}

private typealias CsvRecord = List<String>
private fun File.parseAsGzCsv(onHeader: (Int, String) -> Unit, onRecord: (CsvRecord) -> Unit) {
    if (isFile) GZIPInputStream(inputStream()).bufferedReader().use {
        var count = 0
        it.forEachLine { line ->
            val record: CsvRecord = line.split('\t').map { it.removeSurrounding("\"") }
            if (count == 0)
                record.forEachIndexed(onHeader) else
                onRecord(record)
            count++
        }
    }
}

internal fun ImportJob.options.ClaimType.createIndexes() {
    if (!createIndexes) return
    info("createIndexes for $this")
    if (temp.isNotBlank()) {
        (Document.parse(CreateIndexesJson[this])["indexes"] as? List<*>)?.forEach {
            if (it is Document) {
                info("$temp.createIndex(${it.toJson()})")
                tempCollection.createIndex(it)
            }
        }
    }
}

/**
 * set CREATE_INDEXES_JSON_URL = https://raw.githubusercontent.com/vylegzhanin/simagis-edi/master/toBaseX/src/main/resources/com/simagis/edi/mongodb/
 */
internal object CreateIndexesJson {
    private val url: String? = System.getenv("CREATE_INDEXES_JSON_URL")
    private val lock = ReentrantLock()
    private val cache = mutableMapOf<String, String>()

    private fun ImportJob.options.ClaimType.download(): String = URI(url).let { uri ->
        uri.resolve("$type.createIndexes.json")
                .toURL()
                .openConnection()
                .let { connection ->
                    connection.connect()
                    try {
                        connection
                                .getInputStream()
                                .reader()
                                .readText()
                    } catch (e: Throwable) {
                        if (e !is FileNotFoundException) throw e
                        warning("$uri not found", e)
                        read()
                    }
                }
    }

    private fun ImportJob.options.ClaimType.read(): String = CreateIndexesJson::class.java
            .getResourceAsStream("$type.createIndexes.json")
            ?.use { it.reader().readText() }
            ?: "{}"

    operator fun get(claimType: ImportJob.options.ClaimType): String = when (claimType.createIndexes) {
        true -> lock.withLock {
            cache.getOrPut(claimType.type) {
                when (url) {
                    null -> claimType.read()
                    else -> claimType.download()
                }
            }
        }
        false -> "{}"
    }
}

internal fun ImportJob.options.ClaimType.renameToTarget() {
    info("renameToTarget for $this")
    if (target.isNotBlank()) {
        if (targetCollection.isExists) {
            val now = Date()
            val dateFormat = SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSSS")
            val backupNamePrefix = "$target.backup."
            fun String.parseAsBackupDate(): Date = try {
                dateFormat.parse(removePrefix(backupNamePrefix))
            } catch (e: ParseException) {
                Date(Long.MAX_VALUE)
            }

            val newBackupName = backupNamePrefix + dateFormat.format(now)
            val databaseName = targetCollection.namespace.databaseName
            targetCollection.renameCollection(MongoNamespace(databaseName, newBackupName))
            val database = ImportJob.dbs[databaseName]
            database.listCollectionNames()
                    .filter { it.startsWith(backupNamePrefix) }
                    .filter { it.parseAsBackupDate() < now }
                    .filter { it != newBackupName }
                    .forEach {
                        info("drop old backup $databaseName.$it")
                        database.getCollection(it).drop()
                    }
        }
        tempCollection.renameCollection(targetCollection.namespace)
    }
}
