package com.simagis.edi.mongodb

import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.text.Charsets.UTF_8

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 4/3/2017.
 */
fun main(args: Array<String>) {
    Build835cJob.open(args)
    info("starting job", detailsJson = Build835cJob.jobDoc)
    val docs835 = Build835cJob.options.claimTypes["835"].docs
    val docs835c = Build835cJob.options.claimTypes["835c"].docs

    class Client(val doc: Document) {
        val npi = doc["npi"] as String
    }

    val npiMapClient = mutableMapOf<String, Client>().apply {
        val clients = Build835cJob.claims.getCollection("clientid")
        clients.find().forEach {
            Client(it).let { this[it.npi] = it }
        }
    }

    class Doc837(doc: Document) {
        val json = doc.toJson().toByteArray(UTF_8)
        val doc get() = Document.parse(json.toString(UTF_8))
        val acn = doc["acn"] as? String?
        val sendDate = doc["sendDate"] as? Date?
    }

    val acnMap837 = mutableMapOf<String, MutableList<Doc837>>().apply {
        Build835cJob.options.claimTypes["837"].docs.find()
                .projection(doc {
                    `+`("dx", 1)
                    `+`("npi", 1)
                    `+`("drFirsN", 1)
                    `+`("drLastN", 1)
                    `+`("acn", 1)
                    `+`("sendDate", 1)
                })
                .sort(doc {
                    `+`("sendDate", -1)
                })
                .forEach {
                    val doc837 = Doc837(it)
                    if (doc837.sendDate != null && doc837.acn != null) {
                        getOrPut(doc837.acn, { mutableListOf<Doc837>() }) += doc837
                    }
                }
    }
    docs835c.drop()
    val claimsLeft = AtomicLong(docs835.count())
    Build835cJob.updateProcessing("claimsLeft", claimsLeft.get())
    var docs835cList = mutableListOf<Document>()
    val insertQueue = LinkedBlockingQueue<List<Document>>(5)
    val insertThread = thread(name = "docs835c.insertMany(list)") {
        do {
            val list: List<Document> = insertQueue.poll(5, TimeUnit.SECONDS) ?: continue
            if (list.isEmpty()) break
            docs835c.insertMany(list)
            Build835cJob.updateProcessing("claimsLeft", claimsLeft.addAndGet(-list.size.toLong()))
        } while (true)
    }
    val skipKeys = setOf("_id", "acn")
    docs835.find().forEach { c835 ->
        val procDate = c835["procDate"] as? Date
        val acn = c835["acn"] as? String
        if (procDate != null && acn != null) {
            acnMap837[acn]?.let { list ->
                for (doc837 in list) {
                    if (doc837.sendDate!! < procDate) {
                        doc837.doc.forEach { key, value ->
                            when (key) {
                                "npi" -> {
                                    c835["npi"] = value
                                    c835["client"] = npiMapClient[value]?.doc
                                }
                                !in skipKeys -> c835[key] = value
                            }
                        }
                        break
                    }
                }
            }
            docs835cList.add(c835)
            if (docs835cList.size >= 100) {
                docs835cList.let { list ->
                    docs835cList = mutableListOf<Document>()
                    insertQueue.put(list)
                }
            }
        }
    }

    insertQueue.put(emptyList())
    insertThread.join(1000)
    while (insertThread.isAlive) {
        info("waiting for docs835c.insertMany(list)")
        insertThread.join(5000)
    }

    if (docs835cList.isNotEmpty()) {
        docs835c.insertMany(docs835cList)
        docs835cList = mutableListOf<Document>()
    }
    Build835cJob.updateProcessing("claimsLeft", 0L)
    info("DONE", detailsJson = Build835cJob.jobDoc)
}

private object Build835cJob : AbstractJob() {
    object options {
        private val options: Document by lazy { jobDoc?.get("options") as? Document ?: Document() }

        object claimTypes {
            private val claimTypes: Document by lazy { options["claimTypes"] as? Document ?: Document() }
            private val cache: MutableMap<String, ClaimType> = ConcurrentHashMap()

            operator fun get(type: String): ClaimType = cache.computeIfAbsent(type) {
                val claimType = (claimTypes[type] as? Document) ?: Document()
                ClaimType(
                        type = type,
                        collection = claimType["collection"] as? String ?: "claims_$type")
            }

        }

        data class ClaimType(
                val type: String,
                val collection: String) {
            val docs: DocumentCollection by lazy { claims.getCollection(collection) }
        }
    }
}