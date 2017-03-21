package com.simagis.edi.mdb

import com.mongodb.CursorType
import com.mongodb.MongoClient
import com.mongodb.async.SingleResultCallback
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.CreateCollectionOptions
import org.bson.Document


/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/8/2017.
 */

fun main(args: Array<String>) {
    val mongoClient = MongoClient("mongodb.loc")
    val database = mongoClient.getDatabase("mydb")

    if (database.listCollectionNames().contains("myCappedCollection").not()) {
        database.createCollection("myCappedCollection", CreateCollectionOptions()
                .capped(true)
                .sizeInBytes(100000)
                .maxDocuments(100))
    }
    val collection: MongoCollection<Document> = database.getCollection("myCappedCollection")
    collection.find().cursorType(CursorType.Tailable).iterator().forEach { document ->
        println(document.toJson())
    }
}