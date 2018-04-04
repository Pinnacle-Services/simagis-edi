package com.simagis.edi.mongodb

import com.mongodb.DB
import com.mongodb.DBRef
import com.mongodb.ServerAddress
import com.mongodb.client.MongoCollection
import com.mongodb.gridfs.GridFS
import com.simagis.edi.basex.ISA
import com.simagis.edi.basex.get
import com.simagis.edi.mdb.*
import org.bson.Document
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributeView
import java.security.MessageDigest
import java.util.*

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/20/2017.
 */
fun main(args: Array<String>) {
    val commandLine = com.berryworks.edireader.util.CommandLine(args)
    val _fs = commandLine["fs"] ?: "isa"
    val _mode = commandLine["mode"] ?: "R"
    val _path = File(commandLine[0])

    val mongo = MDBCredentials.mongoClient(
        ServerAddress(
            commandLine["host"] ?: ServerAddress.defaultHost(),
            commandLine["port"]?.toInt() ?: ServerAddress.defaultPort()
        )
    )
    val db = DB(mongo, _fs)

    val fs = GridFS(db)
    val fsDatabase = mongo.getDatabase(_fs)
    val srcCollection = fsDatabase.getCollection("src.files")
    val isaCollection = fsDatabase.getCollection("src.isa")

    val isaIdSet = loadIdSet(isaCollection)

    _path.walk(_mode).forEach { srcFile ->
        try {
            val srcFileDoc = srcCollection.openSrcFileDoc(srcFile)
            if (srcFileDoc["status"] == "CREATING") try {
                ISA.read(srcFile).forEachIndexed { index, isa ->
                    val byteArray = isa.code.toByteArray(ISA.CHARSET)
                    val isaDigest = md().digest(byteArray).toHexString()
                    if (isaIdSet.add(isaDigest)) {
                        val gridFile = fs.createFile(byteArray.inputStream(), isaDigest + ".isa")
                        gridFile.save()
                        isaCollection.insertOne(doc(isaDigest) {
                            `+`("file", DBRef("fs.files", gridFile.get("_id")))
                            `+`("src", DBRef("src.files", srcFileDoc["_id"]))
                            `+`("srcIndex", index)
                            `+`("type", isa.stat.doc.type)
                            `+`("date", isa.stat.doc.date)
                            `+`("time", isa.stat.doc.time)
                            `+`("status", "NEW")
                        })
                        srcCollection.updateOne(
                            doc(srcFileDoc._id),
                            doc { `+$addToSet` { `+`("isas", isaDigest) } })
                    }
                }
                srcCollection.updateStatus(srcFileDoc, "NEW")
            } catch (e: Throwable) {
                e.printStackTrace()
                srcCollection.updateStatus(srcFileDoc, "ERROR", e)
            }
        } catch (e: Throwable) {
            e.printStackTrace()
        }
    }
}

private fun MongoCollection<Document>.updateStatus(doc: Document, status: String, e: Throwable? = null) {
    updateOne(
        doc(doc._id),
        doc {
            `+$set` {
                `+`("status", status)
                if (e != null) {
                    `+`("error", doc {
                        `+`("class", e.javaClass.name)
                        `+`("message", e.message)
                        `+`("stackTrace", StringWriter().let {
                            PrintWriter(it).use { e.printStackTrace(it) }
                            it.toString()
                        })

                    })
                }
            }
        })
}


private fun MongoCollection<Document>.openSrcFileDoc(file: File): Document {
    val path = file.absolutePath
    val srcDigest = file.digest()
    val foundDoc = find(doc(srcDigest)).first()
    if (foundDoc == null) {
        val newDoc = doc(srcDigest) {
            `+`("paths", listOf(path))
            `+`("size", file.length())
            with(Files.getFileAttributeView(file.toPath(), BasicFileAttributeView::class.java).readAttributes()) {
                `+`("created", Date(creationTime().toMillis()))
                `+`("modified", Date(lastModifiedTime().toMillis()))
            }
            `+`("status", "CREATING")
        }
        insertOne(newDoc)
        return newDoc
    } else {
        val updateResult = updateOne(
            doc(srcDigest),
            doc { `+$addToSet` { `+`("paths", path) } })
        if (updateResult.modifiedCount > 0) {
            return find(doc(srcDigest)).first()
        } else {
            return foundDoc
        }
    }
}

private fun loadIdSet(srcCollection: MongoCollection<Document>): MutableSet<String> = mutableSetOf<String>().apply {
    srcCollection.find().forEach({
        this += it["_id"] as String
    })
}

private fun File.digest(): String = md().apply {
    inputStream().use {
        val bytes = ByteArray(4096)
        while (true) {
            val len = it.read(bytes)
            if (len == -1) break
            update(bytes, 0, len)
        }
    }
}.digest().toHexString()


private fun File.walk(mode: String): List<File> = when (mode) {
    "R" -> walk().toList()
    "D" -> listFiles()?.asList() ?: emptyList()
    "F" -> listOf(this)
    else -> emptyList()
}.filter {
    it.isFile
}

private fun md(): MessageDigest = MessageDigest.getInstance("SHA")

private fun ByteArray.toHexString(): String = joinToString(separator = "") {
    Integer.toHexString(it.toInt() and 0xff)
}
