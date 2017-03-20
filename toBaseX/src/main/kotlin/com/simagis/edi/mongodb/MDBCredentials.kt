package com.simagis.edi.mongodb

import com.mongodb.MongoCredential
import java.io.File
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonString
import javax.json.stream.JsonGenerator

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/20/2017.
 */
object MDBCredentials {
    operator fun get(host: String): List<MongoCredential> = map[host] ?: listOf<MongoCredential>()

    private val map: Map<String, List<MongoCredential>> by lazy {
        mutableMapOf<String, List<MongoCredential>>().also { map ->
            val file = File(System.getProperty("user.home")).resolve(".mongo-credentials.json")
            if (!file.exists()) {
                file.writer().use {
                    Json.createWriterFactory(mapOf(JsonGenerator.PRETTY_PRINTING to true))
                            .createWriter(it)
                            .write(Json.createObjectBuilder()
                                    .add("credentials", Json.createObjectBuilder()
                                            .add("localhost", Json.createArrayBuilder()
                                                    .add(Json.createObjectBuilder()
                                                            .add("database", "admin")
                                                            .add("userName", "admin")
                                                            .add("password", "****")
                                                    ))).build())
                }

            }
            val credentials = file.reader().use { Json.createReader(it).readObject() }.getJsonObject("credentials")
            credentials.keys.forEach { host ->
                map[host] = mutableListOf<MongoCredential>().also { array ->
                    credentials.getJsonArray(host).forEach { json ->
                        if (json is JsonObject) {
                            fun JsonObject.opt(name: String, def: String) = (this[name] as? JsonString?)?.string ?: def
                            array += MongoCredential.createScramSha1Credential(
                                    json.opt("userName", "admin"),
                                    json.opt("database", "admin"),
                                    json.opt("password", "").toCharArray()
                            )
                        }
                    }
                }
            }
        }
    }
}

