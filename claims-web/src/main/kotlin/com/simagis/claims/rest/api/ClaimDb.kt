package com.simagis.claims.rest.api

import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.simagis.claims.clientName
import com.simagis.edi.mdb.MDBCredentials
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.io.File
import java.io.IOException
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

val clientsRootDir: File by lazy { File("/PayPredict/clients").absoluteFile.checkDir() }
val clientDir: File by lazy { clientsRootDir.resolve(clientName).checkDir() }

val claimDbRootDir: File by lazy { clientDir.resolve("claim-db").checkDir() }
val claimDbTempDir: File by lazy { claimDbRootDir.resolve("temp").checkDir() }

private fun File.checkDir(): File = apply {
    if (!isDirectory) throw IOException("directory ${this} not found")
}

internal object ClaimDb {
    private val conf: JsonObject by lazy {
        clientDir
            .resolve("conf").checkDir()
            .resolve("claims-db.json")
            .readText()
            .toJsonObject()
    }

    private fun JsonObject.json(name: String): JsonObject =
        this[name] as? JsonObject ?: "{}".toJsonObject()

    private val mongoClient: MongoClient by lazy { MDBCredentials.mongoClient(server) }

    private val db: MongoDatabase by lazy {
        mongoClient.getDatabase(conf.json("api").getString("db", "claimsAPI"))
    }

    val server: ServerAddress by lazy {
        conf.json("mongo").let {
            ServerAddress(
                it.getString("host", ServerAddress.defaultHost()),
                it.getInt("port", ServerAddress.defaultPort())
            )
        }
    }

    val apiJobs: MongoCollection<Document> by lazy { db.getCollection("apiJobs") }
    val cqb: MongoCollection<Document> by lazy { db.getCollection("cqb") }
    val cq: MongoCollection<Document> by lazy { db.getCollection("cq") }

    val ex1: ExecutorService = Executors.newSingleThreadExecutor()

    fun shutdown() {
        ex1.shutdown()
    }
}

internal val jsonPP: JsonWriterFactory = Json.createWriterFactory(
    mapOf(JsonGenerator.PRETTY_PRINTING to true)
)

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