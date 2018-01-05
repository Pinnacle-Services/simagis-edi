package com.simagis.edi.mongodb.ii

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.model.IndexModel
import com.simagis.edi.mdb._id
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import com.simagis.edi.mongodb.*
import com.simagis.edi.mongodb.ImportJob.ii.claims.ClaimType.*
import org.bson.Document
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 1/5/2018.
 */
internal sealed class ClaimsUpdateChannel {
    abstract fun put(claim: IIClaim)
    abstract fun shutdownAndWait()
}

internal class AllClaimsUpdateChannel(options: Options) : ClaimsUpdateChannel() {
    data class Options(
            val sessionId: Long,
            val dateAfter: Date? = null
    )

    private val channel835 = Claims835UpdateChannel(options)
    private val channel835a = AClaims835UpdateChannel(options)
    private val channel837 = Claims837UpdateChannel(options)
    private val channel837a = AClaims837UpdateChannel(options)

    override fun put(claim: IIClaim) {
        if (claim.valid) {
            when (claim.type) {
                "835" -> {
                    channel835.put(claim)
                    channel835a.put(claim)
                }
                "837" -> {
                    channel837.put(claim)
                    channel837a.put(claim)
                }
            }
        }
    }

    override fun shutdownAndWait() {
        channel835.shutdownAndWait()
        channel835a.shutdownAndWait()
        channel837.shutdownAndWait()
        channel837a.shutdownAndWait()
    }
}

private abstract class ClaimsUpdateByMaxDateChannel(val options: AllClaimsUpdateChannel.Options) : ClaimsUpdateChannel(), Runnable {
    private val queue: BlockingQueue<IIClaim> = LinkedBlockingQueue(1024)
    private val thread = Thread(this, "").apply { start() }

    private object ShutdownMarker : IIClaim

    override fun run() {
        val claimsCollection = ImportJob.ii.claims.openCollection(type).indexed()
        var lastClaimId: String? = null
        var maxDateClaim: IIClaim? = null
        while (true) {
            val claim = queue.poll(30, TimeUnit.SECONDS) ?: continue
            if (claim == ShutdownMarker) break
            try {
                val claimId = claim.claim["_id"] as String
                when {
                    lastClaimId == null -> {
                        lastClaimId = claimId
                        maxDateClaim = claim
                    }
                    lastClaimId == claimId -> {
                        if (maxDateClaim == null || claim.date > maxDateClaim.date)
                            maxDateClaim = claim

                    }
                    lastClaimId != claimId -> {
                        maxDateClaim?.insert(claimsCollection)
                        lastClaimId = claimId
                        maxDateClaim = claim
                    }
                }
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }
    }

    protected abstract val type: ImportJob.ii.claims.ClaimType
    protected abstract fun Document.augment(): Document

    protected fun DocumentCollection.indexed(): DocumentCollection = apply {
        val indexes: List<IndexModel> = (Document.parse(CreateIndexesJson[type.name])["indexes"] as? List<*>)
                ?.filterIsInstance<Document>()
                ?.map { IndexModel(it) }
                ?: emptyList()
        createIndexes(indexes + IndexModel(Document("o.s", 1)))
    }

    private fun IIClaim.insert(claimsCollection: DocumentCollection) {
        val claim = claim.augment()
        claim["o"] = doc {
            `+`("s", options.sessionId)
            `+`("d", digest)
        }
        try {
            claimsCollection.insertOne(claim)
        } catch (e: MongoWriteException) {
            if (ErrorCategory.fromErrorCode(e.code) != ErrorCategory.DUPLICATE_KEY) throw e
            val key = doc(claim._id)
            val oldDate = claimsCollection.find(key).first()?.let { date(it) }
            val isReplaceRequired = when {
                oldDate == null -> true
                oldDate < date -> true
                else -> false
            }
            if (isReplaceRequired) {
                claimsCollection.findOneAndReplace(key, claim)
            }
        }
    }

    override fun put(claim: IIClaim) {
        if (options.dateAfter == null || claim.date >= options.dateAfter) {
            queue.put(claim)
        }
    }

    override fun shutdownAndWait() {
        queue.put(ShutdownMarker)
        thread.join(30_000)
        //TODO add warning if thread.isAlive
    }

}

private class Claims835UpdateChannel(options: AllClaimsUpdateChannel.Options) : ClaimsUpdateByMaxDateChannel(options) {
    override val type get() = `835`
    override fun Document.augment(): Document = apply { augment835() }

}

private class AClaims835UpdateChannel(options: AllClaimsUpdateChannel.Options) : ClaimsUpdateByMaxDateChannel(options) {
    override val type get() = `835a`
    override fun Document.augment(): Document = apply { augment835() }
}

private class Claims837UpdateChannel(options: AllClaimsUpdateChannel.Options) : ClaimsUpdateByMaxDateChannel(options) {
    override val type get() = `837`
    override fun Document.augment(): Document = apply { augment837() }
}

private class AClaims837UpdateChannel(options: AllClaimsUpdateChannel.Options) : ClaimsUpdateByMaxDateChannel(options) {
    override val type get() = `837a`
    override fun Document.augment(): Document = apply { augment837() }
}
