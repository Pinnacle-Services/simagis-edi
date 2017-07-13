package com.simagis.edi.mongodb

import com.mongodb.MongoNamespace
import com.mongodb.client.MongoDatabase
import com.simagis.edi.mongodb.dictionary.*
import org.bson.Document
import java.io.File
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ConcurrentHashMap

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
                ClaimType.of(type, (archive[type] as? Document) ?: Document())
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

        object rebuildDicts {
            private val rebuildDicts: Document by lazy { options["rebuildDicts"] as? Document ?: Document() }
            val adjGrp: Dictionary by lazy {
                Dictionary.of<DictionaryBuilder_adjGrp>(dictionary, "dic_adjGrp", rebuildDicts["adjGrp"])
            }
            val adjReason: Dictionary by lazy {
                Dictionary.of<DictionaryBuilder_adjReason>(dictionary, "dic_adjReason", rebuildDicts["adjReason"])
            }
            val cpt: Dictionary by lazy {
                Dictionary.of<DictionaryBuilder_cpt>(dictionary, "dic_cpt", rebuildDicts["cpt"])
            }
            val dxT: Dictionary by lazy {
                Dictionary.of<DictionaryBuilder_dxT>(dictionary, "dic_dxT", rebuildDicts["dxT"])
            }
            val dxV: Dictionary by lazy {
                Dictionary.of<DictionaryBuilder_dxV>(dictionary, "dic_dxV", rebuildDicts["dxV"])
            }
            val frmn: Dictionary by lazy {
                Dictionary.of<DictionaryBuilder_frmn>(dictionary, "dic_frmn", rebuildDicts["frmn"])
            }
            val prn: Dictionary by lazy {
                Dictionary.of<DictionaryBuilder_prn>(dictionary, "dic_prn", rebuildDicts["prn"])
            }
            val rem: Dictionary by lazy {
                Dictionary.of<DictionaryBuilder_rem>(dictionary, "dic_rem", rebuildDicts["rem"])
            }
            val status: Dictionary by lazy {
                Dictionary.of<DictionaryBuilder_status>(dictionary, "dic_status", rebuildDicts["status"])
            }
            val dictionaries = listOf<Dictionary>(
                    adjGrp,
                    adjReason,
                    cpt,
                    dxT,
                    dxV,
                    frmn,
                    prn,
                    rem,
                    status
            )
        }

        data class Dictionary(
                override val db: MongoDatabase,
                override val name: String,
                override val builder: DictionaryBuilder) : DictionaryContext {
            companion object {
                inline fun <reified T : DictionaryBuilder> of(
                        db: MongoDatabase,
                        name: String,
                        options: Any?): Dictionary = Dictionary(
                            db = db,
                            name = name,
                            builder = T::class.java.getDeclaredConstructor(
                                    Document::class.java)
                                    .newInstance(options as? Document ?: Document())
                    )
            }
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
