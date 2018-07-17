package com.simagis.edi.mongodb

import com.mongodb.MongoNamespace
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.IndexModel
import org.bson.Document
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.net.URI
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import java.util.zip.GZIPInputStream
import kotlin.collections.List
import kotlin.collections.Map
import kotlin.collections.MutableMap
import kotlin.collections.Set
import kotlin.collections.filter
import kotlin.collections.forEach
import kotlin.collections.forEachIndexed
import kotlin.collections.getOrPut
import kotlin.collections.map
import kotlin.collections.mutableMapOf
import kotlin.collections.set
import kotlin.concurrent.withLock


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/9/2017.
 */

internal object ImportJob : AbstractJob() {
    val importFiles: DocumentCollection by lazy { claimsAPI.getCollection("importFiles") }

    object options {
        private val options: Document by lazy { jobDoc?.get("options") as? Document ?: Document() }
        val sourceDir: File by lazy { clientDir.checkDir().resolve("sourceFiles").checkDir() }
        val scanMode: String by lazy { "R" }
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
        val restartMemoryLimit: Long by lazy {
            (options["restartMemoryLimit"] as? Number)?.toLong() ?: 500 * 1024* 1024
        }
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

        val plb: ClaimType by lazy {
            ClaimType(
                type = "plb",
                temp = "plb.temp",
                target = "plb",
                createIndexes = false,
                db = claims
            )
        }

        val ptnXQ: Boolean by lazy { options["ptnXQ"] as? Boolean ?: false }

        val ptn_835: ClaimType by lazy {
            ClaimType(
                type = "ptn_835",
                temp = "ptn_835.temp",
                target = "ptn_835",
                createIndexes = false,
                db = ptn
            )
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
            val db: MongoDatabase
        ) {
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

    object ii {
        object sourceClaims {
            fun openSessions(): DocumentCollection = dbs.open("sourceClaims", "sessions")
                .indexed("status")

            fun openFiles(): DocumentCollection = dbs.open("sourceClaims", "files")
                .indexed("session", "status", "size", "names")

            fun openClaims(): DocumentCollection = dbs.open("sourceClaims", "claims")
                .indexed(
                    "session", "files", "type",
                    "claim._id", "claim.procDate", "claim.sendDate"
                )
        }

        object claims {
            fun openOptions(): DocumentCollection = dbs.open("claimsAPI", "options")

            enum class ClaimType { `835`, `837`, `835a`, `837a`, `835c`, `835ac` }

            fun openCollection(type: ClaimType): DocumentCollection = when (type) {
                ClaimType.`835` -> dbs.open("claimsCurrent", "claims_$type")
                ClaimType.`837` -> dbs.open("claimsCurrent", "claims_$type")
                ClaimType.`835a` -> dbs.open("claimsArchive", "claims_$type")
                ClaimType.`837a` -> dbs.open("claimsArchive", "claims_$type")
                ClaimType.`835c` -> dbs.open("claimsCurrent", "claims_$type")
                ClaimType.`835ac` -> dbs.open("claimsArchive", "claims_$type")
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
}

val clientName: String by lazy { System.getProperty("paypredict.client") }
val clientsRootDir: File by lazy { File("/PayPredict/clients").absoluteFile.checkDir() }
val clientDir: File by lazy { clientsRootDir.resolve(clientName).checkDir() }

private fun File.checkDir(): File = apply {
    if (!isDirectory) throw IOException("directory ${this} not found")
}

private fun DocumentCollection.indexed(vararg indexes: String): DocumentCollection = apply {
    createIndexes(indexes.map { IndexModel(Document(it, 1)) })
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

    private fun download(type: String): String = URI(url).let { uri ->
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
                    read(type)
                }
            }
    }

    private fun read(type: String): String = CreateIndexesJson::class.java
        .getResourceAsStream("$type.createIndexes.json")
        ?.use { it.reader().readText() }
            ?: "{}"

    operator fun get(type: String): String = lock.withLock {
        cache.getOrPut(type) {
            when (url) {
                null -> read(type)
                else -> download(type)
            }
        }
    }


    operator fun get(claimType: ImportJob.options.ClaimType): String = when (claimType.createIndexes) {
        true -> get(claimType.type)
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
