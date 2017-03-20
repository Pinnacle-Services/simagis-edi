package com.simagis.edi.mdb

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import org.bson.Document


/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/8/2017.
 */

fun main(args: Array<String>) {
    val mongoClient = MongoClient("mongodb.loc")
    val database = mongoClient.getDatabase("mydb")
    val collection: MongoCollection<Document> = database.getCollection("myCollection")
    val doc = Document("name", "MongoDB")
            .append("type", "database")
            .append("count", 1)
            .append("info", Document("x", 203).append("y", 102))
    println(doc.toJson())
    collection.insertOne(doc)
    println(doc.toJson())
}