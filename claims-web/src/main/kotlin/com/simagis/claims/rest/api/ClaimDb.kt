package com.simagis.claims.rest.api

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.CreateCollectionOptions
import org.bson.Document
import java.io.ByteArrayOutputStream
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

internal object ClaimDb {
    val mongoHost: String = System.getProperty("claims.mongo.host", "127.0.0.1")
    private val mongoDB = System.getProperty("claims.mongo.jobs.db", "claimsAPI")
    private val mongoClient = MongoClient(mongoHost)
    private val db: MongoDatabase = mongoClient.getDatabase(mongoDB)

    val apiJobs: MongoCollection<Document> by lazy { db.getCollection("apiJobs") }
    val apiLog: MongoCollection<Document> by lazy { db.openCappedCollection("apiLog") }
    val cqb: MongoCollection<Document> by lazy { db.getCollection("cqb") }

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


internal fun JsonObject?.toByteArray(): ByteArray = when {
    this == null -> emptyJsonByteArray
    else -> ByteArrayOutputStream().use { jsonPP.createWriter(it).write(this); it }.toByteArray()
}


internal val emptyJsonByteArray = "{}".toByteArray()
internal val emptyJson = Json.createObjectBuilder().build()

internal fun JsonObject?.toDocument(): Document? = if (this == null) null else Document.parse(toString())

internal fun Document?.toJsonObject(): JsonObject? = if (this == null) null else Json.createReader(toJson().reader()).readObject()

internal fun String.toJsonObject(): JsonObject = Json.createReader(reader()).readObject()

internal fun Throwable?.toErrorJsonObject(uuid: String? = null): JsonObject? = if (this == null) null else Json.createObjectBuilder()
        .also {
            it.add("errorClass", this.javaClass.name)
            it.add("message", message)
            it.add("stackTrace", Json.createArrayBuilder().also { array ->
                StringWriter().let {
                    PrintWriter(it).use { printStackTrace(it) }
                    it.toString().lines().forEach { array.add(it) }
                }
            })
            if (uuid != null) it.add("uuid", uuid)
        }.build()

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
