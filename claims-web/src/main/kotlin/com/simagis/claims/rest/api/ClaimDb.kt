package com.simagis.claims.rest.api

import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.CreateCollectionOptions
import com.simagis.edi.mdb.MDBCredentials
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonWriterFactory
import javax.json.stream.JsonGenerator

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/17/2017.
 */

val claimDbRootDir: File = File("/claim-db").absoluteFile
val claimDbTempDir: File by lazy { claimDbRootDir.resolve("temp").apply { mkdir() } }

internal object ClaimDb {
    val mongoHost: String = System.getProperty("claims.mongo.host", "127.0.0.1")
    private val mongoDB = System.getProperty("claims.mongo.jobs.db", "claimsAPI")
    private val mongoClient = MDBCredentials.mongoClient(mongoHost)
    private val db: MongoDatabase = mongoClient.getDatabase(mongoDB)

    val apiJobs: MongoCollection<Document> by lazy { db.getCollection("apiJobs") }
    val apiLog: MongoCollection<Document> by lazy { db.openCappedCollection("apiLog") }
    val cqb: MongoCollection<Document> by lazy { db.getCollection("cqb") }
    val cq: MongoCollection<Document> by lazy { db.getCollection("cq") }

    val ex1: ExecutorService = Executors.newSingleThreadExecutor()

    fun shutdown() {
        ex1.shutdown()
    }
}

internal val jsonPP: JsonWriterFactory = Json.createWriterFactory(mapOf(
        JsonGenerator.PRETTY_PRINTING to true))

internal fun JsonObject?.toStringPP(): String = when {
    this == null -> "{}"
    else -> StringWriter().use { jsonPP.createWriter(it).write(this); it.toString().trim() }
}

internal fun String.toJsonObject(): JsonObject = Json.createReader(reader()).readObject()

internal fun Document.appendError(e: Throwable, uuid: String? = null) {
    `+`("error", doc {
        `+`("errorClass", e.javaClass.name)
        `+`("message", e.message)
        `+`("stackTrace", StringWriter().let {
            PrintWriter(it).use { e.printStackTrace(it) }
            it.toString().lines()
        })
        uuid?.let { `+`("uuid", it) }
    })
}

private val jsonWriterSettingsPP by lazy { JsonWriterSettings(JsonMode.STRICT, true) }
internal fun Document?.toStringPP(): String = when {
    this == null -> "{}"
    else -> toJson(jsonWriterSettingsPP)
}

private val jsonWriterSettingsPPM by lazy { JsonWriterSettings(JsonMode.SHELL, true) }
internal fun Document?.toStringPPM(): String = when {
    this == null -> "{}"
    else -> toJson(jsonWriterSettingsPPM)
}

private fun MongoDatabase.openCappedCollection(collectionName: String,
                                               maxDocuments: Long = 10000,
                                               sizeInBytes: Long = 1024 * 1024): MongoCollection<Document> {
    if (listCollectionNames().contains(collectionName).not()) {
        createCollection(collectionName, CreateCollectionOptions()
                .capped(true)
                .sizeInBytes(sizeInBytes)
                .maxDocuments(maxDocuments))
    }
    return getCollection(collectionName)
}
