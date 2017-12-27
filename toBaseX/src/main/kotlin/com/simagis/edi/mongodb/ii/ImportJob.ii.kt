package com.simagis.edi.mongodb.ii

import com.mongodb.client.MongoIterable
import com.simagis.edi.mdb.*
import com.simagis.edi.mongodb.DocumentCollection
import com.simagis.edi.mongodb.ImportJob
import org.bson.Document
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.lang.StringBuilder
import java.security.MessageDigest
import java.util.*

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 12/22/2017.
 */

@Suppress("unused")
internal fun ImportJob.ii.newSession(): ImportJob.ii.Session = IISession()

internal fun Throwable.toErrorDoc(): Document = doc {
    `+`("class", this@toErrorDoc.javaClass.name)
    `+`("message", message)
    `+`("stackTrace", StringWriter()
            .also { PrintWriter(it).use { printStackTrace(it) } }
            .toString())
}


private interface Updatable {
    val id: Any
    val collection: DocumentCollection
}

private fun Updatable.update(doc: Document, vararg pairs: Pair<String, Any?>) {
    pairs.forEach { pair ->
        with(pair) {
            if (second != null) doc[first] = second else doc.remove(first)
        }
    }
    collection.updateOne(doc(id), updater(pairs))
}


private fun Updatable.update(name: String, value: Any?) {
    collection.updateOne(doc(id), doc {
        if (value != null)
            `+$set` { `+`(name, value) } else
            `+$unset` { `+`(name, "") }
    })
}

private fun Updatable.update(name: String, error: Throwable?) = update(name, error?.toErrorDoc())

private class IISession(sessionId: Long? = null) : ImportJob.ii.Session, Updatable {
    override
    val id: Long = sessionId ?: (maxSessionId() + 1).also {
        ImportJob.ii.sessions.insertOne(doc(it) {
            `+`("createdAt", Date())
            `+`("status", ImportJob.ii.Status.NEW.name)
        })
    }

    override
    val collection: DocumentCollection
        get() = ImportJob.ii.sessions

    private fun maxSessionId(): Long {
        val found = ImportJob.ii.sessions.find(doc {}).sort(doc(-1)).first()?.get("_id")
        return found as? Long ?: 0.toLong()
    }

    override
    var status: ImportJob.ii.Status = ImportJob.ii.Status.NEW
        set(value) {
            update("status", value.name)
            field = value
        }

    override
    var error: Throwable? = null
        set(value) {
            update("error", value)
            field = value
        }

    override
    val files: ImportJob.ii.Files by lazy { IIFiles(this) }

    override
    var filesFound: Int = 0
        set(value) {
            update("filesFound", value)
            field = value
        }
    override
    var filesSucceed: Int = 0
        set(value) {
            update("filesSucceed", value)
            field = value
        }
    override
    var filesFailed: Int = 0
        set(value) {
            update("filesFailed", value)
            field = value
        }
}

private class IIFiles(val session: IISession) : ImportJob.ii.Files {
    val collection: DocumentCollection
        get() = ImportJob.ii.files

    override fun registerFile(file: File): ImportJob.ii.File {
        val fileName = file.name
        val filePath = file.absolutePath
        val fileSize = file.length()

        val foundByName = ImportJob.ii.files.find(doc {
            `+`("names", fileName)
            `+`("size", fileSize)
        }).toList()
        if (foundByName.size == 1) {
            return foundByName.first().toIIFile()
        }

        val id = file.sha2()
        val foundByDigest: Document? = ImportJob.ii.files.find(doc(id)).first()
        return when {
            foundByDigest != null -> {
                val updates = Document()
                fun addToSet(name: String, value: String) {
                    if ((foundByDigest[name] as? List<*>)?.contains(value) != true) {
                        updates.`+$addToSet++` { `+`(name, value) }
                    }
                }
                addToSet("names", fileName)
                addToSet("paths", filePath)
                if (updates.isEmpty()) foundByDigest else {
                    collection.updateOne(doc(id), updates)
                    ImportJob.ii.files.find(doc(id)).first()
                }
            }
            else -> {
                val new: Document = doc(id) {
                    `+`("session", session.id)
                    `+`("status", ImportJob.ii.Status.NEW.name)
                    `+`("size", fileSize)
                    `+`("names", listOf(fileName))
                    `+`("paths", listOf(filePath))
                }
                ImportJob.ii.files.insertOne(new)
                session.filesFound++
                new
            }
        }.toIIFile()
    }

    override
    fun find(status: ImportJob.ii.Status): MongoIterable<ImportJob.ii.File> = collection
            .find(doc { `+`("status", status.name) })
            .sort(doc { `+`("size", -1) })
            .map { it.toIIFile() }

    private fun Document.toIIFile(): IIFile = IIFile(
            files = this@IIFiles,
            id = _id as String,
            doc = this
    )
}

private class IIFile(
        val files: IIFiles,
        override val id: String,
        override val doc: Document) : ImportJob.ii.File, Updatable {

    override val sessionId: Long get() = files.session.id
    override val status: ImportJob.ii.Status get() = enumValueOf(doc["status"] as String)
    override val size: Long = doc["size"] as Long
    override val collection: DocumentCollection get() = ImportJob.ii.files
    override val info: Document? = doc["info"] as? Document
    override val error: Document? = doc["error"] as? Document

    override fun markRunning() {
        update(doc,
                "session" to files.session.id,
                "status" to ImportJob.ii.Status.RUNNING.name,
                "info" to null,
                "error" to null
        )
    }

    override fun markSucceed(info: Document?) {
        files.session.filesSucceed++
        update(doc,
                "session" to files.session.id,
                "status" to ImportJob.ii.Status.SUCCESS.name,
                "info" to info,
                "error" to null
        )
    }

    override fun markFailed(error: Document) {
        files.session.filesFailed++
        update(doc,
                "session" to files.session.id,
                "status" to ImportJob.ii.Status.FAILURE.name,
                "info" to null,
                "error" to error
        )
    }
}

private fun File.sha2() = digest(MessageDigest.getInstance("SHA"))

private fun File.digest(digest: MessageDigest): String {
    digest.update(inputStream().use { stream -> ByteArray(length().toInt()).also { stream.read(it) } })
    return digest.digest().toHexStr()
}

internal fun ByteArray.toHexStr(): String = StringBuilder(size * 2)
        .also { result ->
            for (byte in this) {
                val hex = (byte.toInt() and 0xff).toString(16)
                if (hex.length > 1) {
                    result.append(hex)
                } else {
                    result.append('0')
                    result.append(hex)
                }
            }
        }
        .toString()
