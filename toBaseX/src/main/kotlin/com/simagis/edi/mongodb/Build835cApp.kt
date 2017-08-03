package com.simagis.edi.mongodb

import com.simagis.edi.mdb._id
import com.simagis.edi.mdb.`+$set`
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.system.exitProcess
import kotlin.text.Charsets.UTF_8

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 4/3/2017.
 */
fun main(args: Array<String>) {
    ImportJob.open(args)
    info("starting job", detailsJson = ImportJob.jobDoc)
    if (ImportJob.options.build835c._835c.temp.isBlank()) {
        info("SKIP build835c")
        exitProcess(0)
    }

    val docs835 = ImportJob.options.claimTypes["835"].targetCollection
    val docs837 = ImportJob.options.claimTypes["837"].targetCollection
    val docs835c = ImportJob.options.build835c._835c.tempCollection

    class Client(doc: Document) {
        val doc = Document(doc).apply { remove("_id") }
        val npi = doc["npi"] as String
    }

    val npiMapClient = mutableMapOf<String, Client>().apply {
        val clients = ImportJob.options.build835c.clients
        clients.find().forEach {
            Client(it).let { this[it.npi] = it }
        }
    }


    class Ref835(val id: String, val procDate: Date) {
        fun toDoc(): Document = Document().apply {
            this["id835"] = id
            this["procDate"] = procDate
        }
    }

    class EOB837(doc: Document) {
        private val map = mutableMapOf<String, Ref835>().apply {
            (doc["eob"] as? List<*>)?.forEach {
                if (it is Document) {
                    it.toRef835("id835")?.let { this[it.id] = it }
                }
            }
        }
        val _id = doc._id
        var modified: Boolean = false
            private set(value) {
                field = value
            }

        private fun Document?.toRef835(_id: String = "_id"): Ref835? {
            if (this == null) return null
            val id = this[_id] as? String
            val procDate = this["procDate"] as? Date
            return when {
                id != null && procDate != null -> Ref835(id, procDate)
                else -> null
            }
        }

        operator fun plusAssign(doc835: Document) {
            doc835.toRef835()?.let { it ->
                if (map.put(it.id, it) == null) {
                    modified = true
                }
            }
        }

        fun toList(): List<Document> = map.values.sortedBy { it.procDate }.map { it.toDoc() }
    }

    class Doc837(doc: Document) {
        val json = doc.toJson().toByteArray(UTF_8)
        val doc get() = Document.parse(json.toString(UTF_8))
        val acn = doc["acn"] as? String?
        val sendDate = doc["sendDate"] as? Date?
        val eob = EOB837(doc)
    }

    val acnMap837 = mutableMapOf<String, MutableList<Doc837>>().apply {
        docs837.find()
                .projection(doc {
                    `+`("dx", 1)
                    `+`("npi", 1)
                    `+`("drFirsN", 1)
                    `+`("drLastN", 1)
                    `+`("ptnBd", 1)
                    `+`("ptnG", 1)
                    `+`("acn", 1)
                    `+`("sendDate", 1)
                    `+`("eob", 1)
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
    ImportJob.updateProcessing("claimsLeft", claimsLeft.get())
    var docs835cList = mutableListOf<Document>()
    val insertQueue = LinkedBlockingQueue<List<Document>>(5)
    val insertThread = thread(name = "docs835c.insertMany(list)") {
        do {
            val list: List<Document> = insertQueue.poll(5, TimeUnit.SECONDS) ?: continue
            if (list.isEmpty()) break
            docs835c.insertMany(list)
            ImportJob.updateProcessing("claimsLeft", claimsLeft.addAndGet(-list.size.toLong()))
        } while (true)
    }
    val skipKeys = setOf("_id", "acn", "eob")
    docs835.find().forEach { c835 ->
        val procDate = c835["procDate"] as? Date
        val acn = c835["acn"] as? String
        if (procDate != null && acn != null) {
            acnMap837[acn]?.let { list ->
                for (doc837 in list) {
                    if (doc837.sendDate!! < procDate) {
                        doc837.eob += c835
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
    ImportJob.updateProcessing("claimsLeft", 0L)

    info("updating docs837.eob")
    acnMap837.values.forEach {
        it.forEach {
            if (it.eob.modified) {
                docs837.updateOne(doc(it.eob._id), doc {
                    `+$set` { `+`("eob", it.eob.toList()) }
                })
            }
        }
    }
    info("updating docs837.eob DONE")

    with(ImportJob.options.build835c._835c) {
        createIndexes()
        renameToTarget()
    }

    info("DONE", detailsJson = ImportJob.jobDoc)
}