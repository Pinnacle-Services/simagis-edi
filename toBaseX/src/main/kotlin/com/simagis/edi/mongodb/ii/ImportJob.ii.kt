package com.simagis.edi.mongodb.ii

import com.mongodb.client.MongoIterable
import com.mongodb.client.model.FindOneAndReplaceOptions
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
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 12/22/2017.
 */

@Suppress("unused")
internal fun ImportJob.ii.newSession(): ImportJob.ii.Session = IISession()

@Suppress("unused")
internal fun ImportJob.ii.getClaims(): ImportJob.ii.Claims = IIClaims()

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
        ImportJob.ii.sourceClaims.sessions.insertOne(doc(it) {
            `+`("createdAt", Date())
            `+`("status", ImportJob.ii.Status.NEW.name)
        })
    }

    override
    val collection: DocumentCollection
        get() = ImportJob.ii.sourceClaims.sessions

    private fun maxSessionId(): Long {
        val found = ImportJob.ii.sourceClaims.sessions.find(doc {}).sort(doc(-1)).first()?.get("_id")
        return found as? Long ?: 0.toLong()
    }

    override
    var status: ImportJob.ii.Status = ImportJob.ii.Status.NEW
        set(value) {
            update("status", value.name)
            field = value
        }

    override
    var step: String? = null
        set(value) {
            update("step", value)
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
        get() = ImportJob.ii.sourceClaims.files

    override fun registerFile(file: File): ImportJob.ii.File {
        val fileName = file.name
        val filePath = file.absolutePath
        val fileSize = file.length()

        val foundByName = ImportJob.ii.sourceClaims.files.find(doc {
            `+`("names", fileName)
            `+`("size", fileSize)
        }).toList()
        if (foundByName.size == 1) {
            return foundByName.first().toIIFile()
        }

        val id = file.sha2()
        val foundByDigest: Document? = ImportJob.ii.sourceClaims.files.find(doc(id)).first()
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
                    ImportJob.ii.sourceClaims.files.find(doc(id)).first()
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
                ImportJob.ii.sourceClaims.files.insertOne(new)
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
    override val collection: DocumentCollection get() = ImportJob.ii.sourceClaims.files
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

private class IIClaims : ImportJob.ii.Claims {
    private val iiStatus: DocumentCollection = ImportJob.ii.claims.current.db["iiStatus"]
    private val newMaxSessionId = AtomicLong()

    override fun findNew(): MongoIterable<ImportJob.ii.Claim> {
        val maxSessionId = iiStatus.find(doc("current")).first()?.get("maxSessionId") as? Long
        newMaxSessionId.set(maxSessionId ?: 0)
        return ImportJob.ii.sourceClaims.claims
                .find(doc { if (maxSessionId != null) `+$gt`("session", maxSessionId) })
                .sort(doc { `+`("claims._id", 1) })
                .map { doc ->
                    (doc["session"] as? Long)?.let { session ->
                        newMaxSessionId.getAndUpdate { max(it, session) }
                    }
                    val type = doc["type"] as? String
                    when (type) {
                        "835" -> IIClaim835(doc)
                        "837" -> IIClaim837(doc)
                        else -> IIClaimInvalid(doc)
                    }
                }
    }

    override fun commit() {
        val key = doc("current")
        val current = iiStatus.find(key).first() ?: doc("current")
        current["maxSessionId"] = newMaxSessionId.get()
        iiStatus.findOneAndReplace(key, current, FindOneAndReplaceOptions().upsert(true))
    }
}

private sealed class IIClaim(val doc: Document) : ImportJob.ii.Claim {
    override val claim: Document = doc["claim"] as Document
    private val _date: Date? by lazy { date(claim) }
    override val date: Date get() = _date!!
    override val valid: Boolean = _date != null
}

private class IIClaim835(doc: Document) : IIClaim(doc) {
    override val type: String = "835"
    override fun date(claim: Document): Date? = claim["procDate"] as? Date
}

private class IIClaim837(doc: Document) : IIClaim(doc) {
    override val type: String = "837"
    override fun date(claim: Document): Date? = claim["sendDate"] as? Date
}

private class IIClaimInvalid(doc: Document) : IIClaim(doc) {
    override val valid: Boolean = false
    override val type: String = "???"
    override fun date(claim: Document): Date? = throw AssertionError()
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
