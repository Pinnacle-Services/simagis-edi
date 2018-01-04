package com.simagis.edi.mongodb.ii

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.model.IndexModel
import com.simagis.edi.mdb._id
import com.simagis.edi.mdb.doc
import com.simagis.edi.mdb.get
import com.simagis.edi.mongodb.DocumentCollection
import com.simagis.edi.mongodb.ImportJob
import com.simagis.edi.mongodb.augment835
import com.simagis.edi.mongodb.augment837
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

internal class AllClaimsUpdateChannel(dateAfter: Date?) : ClaimsUpdateChannel() {
    private val channel835 = Claims835UpdateChannel(dateAfter)
    private val channel835a = AClaims835UpdateChannel()
    private val channel837 = Claims837UpdateChannel(dateAfter)
    private val channel837a = AClaims837UpdateChannel()

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

private abstract class ClaimsUpdateByMaxDateChannel : ClaimsUpdateChannel(), Runnable {
    private val queue: BlockingQueue<IIClaim> = LinkedBlockingQueue(10)
    private val thread = Thread(this, "").apply { start() }

    private object ShutdownMarker : IIClaim

    override fun run() {
        val claimsCollection = openClaimsCollection()
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

    protected abstract val type: String
    protected abstract fun openClaimsCollection(): DocumentCollection
    protected abstract fun Document.augment(): Document
    protected abstract val dateAfter: Date?

    protected fun DocumentCollection.indexed(): DocumentCollection = apply {
        val indexes: List<IndexModel> = (org.bson.Document.parse(com.simagis.edi.mongodb.CreateIndexesJson[type])["indexes"] as? List<*>)
                ?.filterIsInstance<Document>()
                ?.map { com.mongodb.client.model.IndexModel(it) }
                ?: kotlin.collections.emptyList()
        createIndexes(indexes)
    }

    private fun IIClaim.insert(claimsCollection: DocumentCollection) {
        val claim = claim.augment()
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
        if (dateAfter == null || claim.date >= dateAfter) {
            queue.put(claim)
        }
    }

    override fun shutdownAndWait() {
        queue.put(ShutdownMarker)
        thread.join(30_000)
        //TODO add warning if thread.isAlive
    }

}

private class Claims835UpdateChannel(override val dateAfter: Date?) : ClaimsUpdateByMaxDateChannel() {
    override val type: String = "835"
    override fun openClaimsCollection(): DocumentCollection = ImportJob.ii.claims.current.openDb()["claims_$type"].indexed()
    override fun Document.augment(): Document = apply { augment835() }

}

private class AClaims835UpdateChannel : ClaimsUpdateByMaxDateChannel() {
    override val type: String = "835"
    override fun openClaimsCollection(): DocumentCollection = ImportJob.ii.claims.archive.openDb()["claims_${type}a"].indexed()
    override fun Document.augment(): Document = apply { augment835() }
    override val dateAfter: Date? = null
}

private class Claims837UpdateChannel(override val dateAfter: Date?) : ClaimsUpdateByMaxDateChannel() {
    override val type: String = "837"
    override fun openClaimsCollection(): DocumentCollection = ImportJob.ii.claims.current.openDb()["claims_$type"].indexed()
    override fun Document.augment(): Document = apply { augment837() }
}

private class AClaims837UpdateChannel : ClaimsUpdateByMaxDateChannel() {
    override val type: String = "837"
    override fun openClaimsCollection(): DocumentCollection = ImportJob.ii.claims.archive.openDb()["claims_${type}a"].indexed()
    override fun Document.augment(): Document = apply { augment837() }
    override val dateAfter: Date? = null
}
