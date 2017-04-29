package com.simagis.edi.mongodb

import org.bson.Document
import java.io.File
import java.util.concurrent.ConcurrentHashMap

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/9/2017.
 */

internal object ImportJob : AbstractJob() {
    object digest {
        val isa: DocumentCollection by lazy { claimsAPI.getCollection("digest.isa") }
        /*
        {
          "_id": "0f6c0903ca5e2f45bf53d6d3274379b5afee3aba",
          "files": [
              "15a3fc927d2cce8ec84d2f040d1de6a70593aadf",
              "16b55fe965458e554d9ccccb52371eb9c055627f"],
          "status": "NEW" // "NEW" | "ARCIVED" | "ERROR"
        }
         */

        val file: DocumentCollection by lazy { claimsAPI.getCollection("digest.file") }
        /*
        {
          "_id": "15a3fc927d2cce8ec84d2f040d1de6a70593aadf",
          "names": ["835Ansi_from_flat.6528.txt_001.txt"],
          "status": "NEW" // "NEW" | "ARCIVED" | "ERROR"
        }
         */
    }

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
                claims.getCollection(build835c["clients"] as? String ?: "clientid" )
            }
        }

        data class ClaimType(
                val type: String,
                val name: String,
                val createIndexes: Boolean) {
            val collection: DocumentCollection by lazy { claims.getCollection(name) }
            companion object {
                internal fun of(type: String, claimType: Document) = ClaimType(
                        type = type,
                        name = claimType["name"] as? String ?: "claims_$type",
                        createIndexes = claimType["createIndexes"] as? Boolean ?: true
                )
            }
        }

    }
}

internal fun ImportJob.options.ClaimType.createIndexes() {
    if (!createIndexes) return
    info("createIndexes for $this")
    (Document.parse(ImportJob::class.java
            .getResourceAsStream("$type.createIndexes.json")
            ?.use { it.reader().readText() }
            ?: "{}")
            ["indexes"] as? List<*>)
            ?.forEach {
                if (it is Document) {
                    info("$name.createIndex(${it.toJson()})")
                    collection.createIndex(it)
                }
            }
}
