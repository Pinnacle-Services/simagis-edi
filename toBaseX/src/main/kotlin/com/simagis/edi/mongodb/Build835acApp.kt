package com.simagis.edi.mongodb

import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import java.util.*
import java.util.concurrent.*
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
    if (ImportJob.options.build835ac._835ac.temp.isBlank()) {
        info("SKIP build835ac")
        exitProcess(0)
    }

    val docs835a = ImportJob.options.archive["835a"].targetCollection
    val docs837a = ImportJob.options.archive["837a"].targetCollection
    val docs835ac = ImportJob.options.build835ac._835ac.tempCollection

    class Client(doc: Document) {
        val doc = Document(doc).apply { remove("_id") }
        val npi = doc["npi"] as String
    }

    val npiMapClient = mutableMapOf<String, Client>().apply {
        val clients = ImportJob.options.build835ac.clients
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
        docs837a.find()
                .projection(doc {
                    `+`("dx", 1)
                    `+`("npi", 1)
                    `+`("drFirsN", 1)
                    `+`("drLastN", 1)
                    `+`("ptnBd", 1)
                    `+`("ptnG", 1)
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
    docs835ac.drop()
    val claimsLeft = AtomicLong(docs835a.count())
    ImportJob.updateProcessing("claimsLeft", claimsLeft.get())
    var docs835acList = mutableListOf<Document>()
    val insertQueue = LinkedBlockingQueue<List<Document>>(5)
    val insertThread = thread(name = "docs835ac.insertMany(list)") {
        do {
            val list: List<Document> = insertQueue.poll(5, TimeUnit.SECONDS) ?: continue
            if (list.isEmpty()) break
            docs835ac.insertMany(list)
            ImportJob.updateProcessing("claimsLeft", claimsLeft.addAndGet(-list.size.toLong()))
        } while (true)
    }
    val skipKeys = setOf("_id", "acn")
    docs835a.find().forEach { c835 ->
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
            docs835acList.add(c835)
            if (docs835acList.size >= 100) {
                docs835acList.let { list ->
                    docs835acList = mutableListOf<Document>()
                    insertQueue.put(list)
                }
            }
        }
    }

    insertQueue.put(emptyList())
    insertThread.join(1000)
    while (insertThread.isAlive) {
        info("waiting for docs835ac.insertMany(list)")
        insertThread.join(5000)
    }

    if (docs835acList.isNotEmpty()) {
        docs835ac.insertMany(docs835acList)
        docs835acList = mutableListOf<Document>()
    }
    ImportJob.updateProcessing("claimsLeft", 0L)

    with(ImportJob.options.build835ac._835ac) {
        createIndexes()
        renameToTarget()
    }

    info("DONE", detailsJson = ImportJob.jobDoc)
}