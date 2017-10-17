package com.simagis.edi.mongodb

import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.ListCollectionsIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.MongoIterable
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.CreateViewOptions
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 10/18/2017.
 */
fun main(args: Array<String>) {
    val none: MongoDatabase = object : MongoDatabase {
        override fun runCommand(command: Bson?): Document {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun runCommand(command: Bson?, readPreference: ReadPreference?): Document {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun <TResult : Any?> runCommand(command: Bson?, resultClass: Class<TResult>?): TResult {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun <TResult : Any?> runCommand(command: Bson?, readPreference: ReadPreference?, resultClass: Class<TResult>?): TResult {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun getName(): String {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun withCodecRegistry(codecRegistry: CodecRegistry?): MongoDatabase {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun drop() {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun getCodecRegistry(): CodecRegistry {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun withReadConcern(readConcern: ReadConcern?): MongoDatabase {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun createView(viewName: String?, viewOn: String?, pipeline: MutableList<out Bson>?) {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun createView(viewName: String?, viewOn: String?, pipeline: MutableList<out Bson>?, createViewOptions: CreateViewOptions?) {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun listCollectionNames(): MongoIterable<String> {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun listCollections(): ListCollectionsIterable<Document> {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun <TResult : Any?> listCollections(resultClass: Class<TResult>?): ListCollectionsIterable<TResult> {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun withReadPreference(readPreference: ReadPreference?): MongoDatabase {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun withWriteConcern(writeConcern: WriteConcern?): MongoDatabase {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun createCollection(collectionName: String?) {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun createCollection(collectionName: String?, createCollectionOptions: CreateCollectionOptions?) {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun getCollection(collectionName: String?): MongoCollection<Document> {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun <TDocument : Any?> getCollection(collectionName: String?, documentClass: Class<TDocument>?): MongoCollection<TDocument> {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun getWriteConcern(): WriteConcern {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun getReadConcern(): ReadConcern {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun getReadPreference(): ReadPreference {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }
    }
    logger = { level: LogLevel, message: String, error: Throwable?, details: String?, detailsJson: Any?, detailsXml: String? ->
        println("*********************")
        println(level)
        println(message)
        error?.printStackTrace()
        println("*********************")
    }
    println("835a = " + CreateIndexesJson[ImportJob.options.ClaimType("835a", "", "", true, none)])
    println("835a2 = " + CreateIndexesJson[ImportJob.options.ClaimType("835a2", "", "", true, none)])
}