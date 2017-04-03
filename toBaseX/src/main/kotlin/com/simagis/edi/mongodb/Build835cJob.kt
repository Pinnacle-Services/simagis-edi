package com.simagis.edi.mongodb

import com.simagis.edi.mdb.`+$lt`
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 4/3/2017.
 */
fun main(args: Array<String>) {
    Build835cJob.open(args)
    info("starting job", detailsJson = Build835cJob.jobDoc)
    val docs835 = Build835cJob.options.claimTypes["835"].docs
    val docs837 = Build835cJob.options.claimTypes["837"].docs
    val docs835c = Build835cJob.options.claimTypes["835c"].docs
    docs835c.drop()
    var claimsLeft = docs835.count()
    Build835cJob.updateProcessing("claimsLeft", claimsLeft)
    docs835.find().forEach { c835 ->
        val c835procDate = c835["procDate"] as? Date
        if (c835procDate != null) {
            val c837 = docs837.find(doc {
                `+`("acn", c835["acn"])
                `+$lt`("sendDate", c835procDate)
            }).projection(doc {
                `+`("dx", 1)
                `+`("npi", 1)
                `+`("drFirsN", 1)
                `+`("drLastN", 1)
                `+`("sendDate", 1)
            }).sort(doc {
                `+`("sendDate", -1)
            }).first()
            if (c837 != null) {
                c835["c837"] = c837
                docs835c.insertOne(c835)
            }
        }
        if (claimsLeft-- % 1000L == 0L) {
            Build835cJob.updateProcessing("claimsLeft", claimsLeft)
        }
    }
    Build835cJob.updateProcessing("claimsLeft", 0)
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