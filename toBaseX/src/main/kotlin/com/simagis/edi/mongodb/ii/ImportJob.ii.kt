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
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.max

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 12/22/2017.
 */

internal fun ImportJob.ii.newSession(): IISession = this.let { IISessionImpl() }

internal fun ImportJob.ii.getClaims(): IIClaims = this.let { IIClaimsImpl() }

internal fun Throwable.toErrorDoc(): Document = doc {
    `+`("class", this@toErrorDoc.javaClass.name)
    `+`("message", message)
    `+`("stackTrace", StringWriter()
            .also { PrintWriter(it).use { printStackTrace(it) } }
            .toString())
}


private class SyncCollection(
        val collection: DocumentCollection,
        private val lock: ReentrantLock = ReentrantLock()) {

    operator fun <T> invoke(block: SyncCollection.() -> T) = lock.withLock { block() }
}

private interface Updatable {
    val sync: SyncCollection
    val id: Any
}

private fun Updatable.update(doc: Document, vararg pairs: Pair<String, Any?>) = sync {
    pairs.forEach { pair ->
        with(pair) {
            if (second != null) doc[first] = second else doc.remove(first)
        }
    }
    collection.updateOne(doc(id), updater(pairs))
}


private fun Updatable.update(name: String, value: Any?) = sync {
    collection.updateOne(doc(id), doc {
        if (value != null)
            `+$set` { `+`(name, value) } else
            `+$unset` { `+`(name, "") }
    })
}

private fun Updatable.update(name: String, error: Throwable?) = update(name, error?.toErrorDoc())

private class IISessionImpl(sessionId: Long? = null) : IISession, Updatable {
    override val sync: SyncCollection = SyncCollection(ImportJob.ii.sourceClaims.openSessions())

    override val id: Long = sync {
        sessionId ?: (maxSessionId() + 1).also { newSessionId ->
            sync {
                collection.insertOne(doc(newSessionId) {
                    `+`("createdAt", Date())
                    `+`("status", IIStatus.NEW.name)
                })
            }
        }
    }

    private fun SyncCollection.maxSessionId(): Long {
        val found = collection.find(doc {}).sort(doc(-1)).first()?.get("_id")
        return found as? Long ?: 0.toLong()
    }

    override var status: IIStatus = IIStatus.NEW
        set(value) {
            update("status", value.name)
            field = value
        }

    override var step: String? = null
        set(value) {
            update("step", value)
            field = value
        }

    override var error: Throwable? = null
        set(value) {
            update("error", value)
            field = value
        }

    override val files: IIFiles by lazy { IIFilesImpl(this) }

    override var filesFound: Int = 0
        set(value) {
            update("filesFound", value)
            field = value
        }

    override var filesSucceed: Int = 0
        set(value) {
            update("filesSucceed", value)
            field = value
        }

    override var filesFailed: Int = 0
        set(value) {
            update("filesFailed", value)
            field = value
        }
}

private class IIFilesImpl(val session: IISessionImpl) : IIFiles {
    val sync: SyncCollection = SyncCollection(ImportJob.ii.sourceClaims.openFiles())

    override fun registerFile(file: File): IIFile = sync {
        val fileName = file.name
        val filePath = file.absolutePath
        val fileSize = file.length()

        val foundByName = collection.find(doc {
            `+`("names", fileName)
            `+`("size", fileSize)
        }).toList()
        if (foundByName.size == 1) {
            return@sync foundByName.first().toIIFile()
        }

        val id = file.sha2()
        val foundByDigest: Document? = collection.find(doc(id)).first()
        return@sync when {
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
                    collection.find(doc(id)).first()
                }
            }
            else -> {
                val new: Document = doc(id) {
                    `+`("session", session.id)
                    `+`("status", IIStatus.NEW.name)
                    `+`("size", fileSize)
                    `+`("names", listOf(fileName))
                    `+`("paths", listOf(filePath))
                }
                collection.insertOne(new)
                session.filesFound++
                new
            }
        }.toIIFile()
    }

    override fun find(status: IIStatus): MongoIterable<IIFile> = sync {
        collection
                .find(doc { `+`("status", status.name) })
                .sort(doc { `+`("size", -1) })
                .map { it.toIIFile() }
    }

    private fun Document.toIIFile(): IIFileImpl = IIFileImpl(
            files = this@IIFilesImpl,
            id = _id as String,
            doc = this
    )
}

private class IIFileImpl(
        val files: IIFilesImpl,
        override val id: String,
        override val doc: Document) : IIFile, Updatable {
    override val sync: SyncCollection = SyncCollection(ImportJob.ii.sourceClaims.openFiles())
    override val sessionId: Long get() = files.session.id
    override val status: IIStatus get() = enumValueOf(doc["status"] as String)
    override val size: Long = doc["size"] as Long
    override val info: Document? = doc["info"] as? Document
    override val error: Document? = doc["error"] as? Document

    override fun markRunning() {
        update(doc,
                "session" to files.session.id,
                "status" to IIStatus.RUNNING.name,
                "info" to null,
                "error" to null
        )
    }

    override fun markSucceed(info: Document?) {
        files.session.filesSucceed++
        update(doc,
                "session" to files.session.id,
                "status" to IIStatus.SUCCESS.name,
                "info" to info,
                "error" to null
        )
    }

    override fun markFailed(error: Document) {
        files.session.filesFailed++
        update(doc,
                "session" to files.session.id,
                "status" to IIStatus.FAILURE.name,
                "info" to null,
                "error" to error
        )
    }
}

private class IIClaimsImpl : IIClaims {
    private val iiStatus: DocumentCollection = ImportJob.ii.claims.current.openDb()["iiStatus"]
    private val newMaxSessionId = AtomicLong()

    override fun findNew(): MongoIterable<IIClaim> {
        val maxSessionId = iiStatus.find(doc("current")).first()?.get("maxSessionId") as? Long
        newMaxSessionId.set(maxSessionId ?: 0)
        return ImportJob.ii.sourceClaims
                .openClaims()
                .find(doc { if (maxSessionId != null) `+$gt`("session", maxSessionId) })
                .sort(doc { `+`("claim._id", 1) })
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

private sealed class IIClaimSealed(val doc: Document) : IIClaim {
    override val claim: Document = doc["claim"] as Document
    private val _date: Date? by lazy { date(claim) }
    override val date: Date get() = _date!!
    override val valid: Boolean = _date != null
}

private class IIClaim835(doc: Document) : IIClaimSealed(doc) {
    override val type: String = "835"
    override fun date(claim: Document): Date? = claim["procDate"] as? Date
}

private class IIClaim837(doc: Document) : IIClaimSealed(doc) {
    override val type: String = "837"
    override fun date(claim: Document): Date? = claim["sendDate"] as? Date
}

private class IIClaimInvalid(doc: Document) : IIClaimSealed(doc)

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
