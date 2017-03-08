package com.simagis.edi.mongodb

import com.mongodb.ErrorCategory
import com.mongodb.MongoClient
import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.simagis.edi.basex.exit
import com.simagis.edi.basex.get
import org.bson.Document
import java.io.File
import javax.json.Json
import javax.json.JsonObject

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/9/2017.
 */
fun main(args: Array<String>) {
    val commandLine = com.berryworks.edireader.util.CommandLine(args)
    val host = commandLine["host"] ?: "localhost"
    val db = commandLine["db"] ?: "importTest"
    val c = commandLine["c"] ?: "importFromJsonArray"

    if (commandLine.size() == 0)
        exit("""
        Usage: importFromJsonArray.kt [-host host] [-db database] [-c collection] <file>
            host: $host
            database: $db
            collection: $c""")

    val file = File(commandLine[0])

    println("Starting import $file into $host $db $c")

    val mongoClient = MongoClient(host)
    val database = mongoClient.getDatabase(db)
    val collection: MongoCollection<Document> = database.getCollection(c)

    val jsonArray = file.inputStream().use {
        Json.createReader(it).readArray()
    }

    jsonArray.forEach {
        if (it is JsonObject) {
            val document = Document.parse(it.toString())
            try {
                collection.insertOne(document)
            } catch(e: MongoWriteException) {
                if (ErrorCategory.fromErrorCode(e.code) == ErrorCategory.DUPLICATE_KEY)
                    println("DUPLICATE") else
                    throw e
            }
            println(document.toJson())
        }
    }
}