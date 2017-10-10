package com.simagis.edi.mongodb

import com.mongodb.MongoNamespace
import com.mongodb.client.MongoDatabase
import org.bson.Document
import java.io.File
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.GZIPInputStream

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
                claims.getCollection(build835c["clients"] as? String ?: "clientid" )
            }
        }

        object build835ac {
            private val build835ac: Document by lazy { options["build835ac"] as? Document ?: Document() }
            val _835ac: ClaimType by lazy {
                ClaimType.of("835ac", build835ac["835ac"] as? Document ?: Document(), claimsA)
            }
            val clients: DocumentCollection by lazy {
                claims.getCollection(build835ac["clients"] as? String ?: "clientid" )
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
                File("files").resolve("xifin_billed_acn.gzip").let { gzipFile ->
                    if (gzipFile.isFile) {
                        GZIPInputStream(gzipFile.inputStream()).bufferedReader().use {
                            val header = mutableMapOf<String, Int>()
                            it.forEachLine { line ->
                                val record: List<String> = line.split('\t')
                                if (header.isEmpty()) {
                                    record.forEachIndexed { index, name ->
                                        header[name.removeSurrounding("\"")] = index
                                    }
                                }
                                else {
                                    operator fun List<String>.get(name: String): String? = header[name]?.let {
                                        elementAtOrNull(it)?.removeSurrounding("\"")
                                    }
                                    val id = record["id"]
                                    val prid = record["prid"]
                                    val prg = record["prg"]
                                    if (id != null && prid != null && prg != null) {
                                        this[id] = doc(id, prid, prg)
                                    }
                                }
                            }
                        }
                    }
                }
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

internal fun ImportJob.options.ClaimType.createIndexes() {
    if (!createIndexes) return
    info("createIndexes for $this")
    if (temp.isNotBlank()) {
        (Document.parse(ImportJob::class.java
                .getResourceAsStream("$type.createIndexes.json")
                ?.use { it.reader().readText() }
                ?: "{}")
                ["indexes"] as? List<*>)
                ?.forEach {
                    if (it is Document) {
                        info("$temp.createIndex(${it.toJson()})")
                        tempCollection.createIndex(it)
                    }
                }
    }
}

internal fun ImportJob.options.ClaimType.renameToTarget() {
    info("renameToTarget for $this")
    if (target.isNotBlank()) {
        if (targetCollection.isExists) {
            targetCollection.renameCollection(
                    MongoNamespace(ImportJob.claims.name, target
                            + ".backup." + SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSSS").format(Date()))
            )
        }
        tempCollection.renameCollection(targetCollection.namespace)
    }
}
