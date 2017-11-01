package com.simagis.edi.mongodb

import com.mongodb.DBRef
import com.mongodb.client.model.UpdateOptions
import com.simagis.edi.mdb.*
import java.util.*

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 11/1/2017.
 */
fun main(args: Array<String>) {
    ImportJob.open(args)
    info("starting job", detailsJson = ImportJob.jobDoc)

    val paya_modified_claims_name = "paya_modified_claims"
    val paya_claim_tags_name = "paya_claim_tags"

    val noEobRef: DBRef = ImportJob.claims[paya_claim_tags_name].let { collection ->
        collection.find(doc { `+`("name", "no-eob") }).firstOrNull().let {
            when (it) {
                null -> {
                    val new = doc {
                        `+`("_class", "com.bioreference.paya.domain.ClaimTag")
                        `+`("name", "no-eob")
                        `+`("color", "#d8651e")
                    }
                    collection.insertOne(new)
                    DBRef(collection.namespace.collectionName, new._id)
                }
                else -> DBRef(collection.namespace.collectionName, it._id)
            }
        }
    }

    val paya_modified_claims: DocumentCollection = ImportJob.claims[paya_modified_claims_name]

    val docs837 = ImportJob.options.claimTypes["837"].targetCollection
    val claimCollection = docs837.namespace.collectionName
    docs837.find(
            doc {
                `+`("noEob", true)
                `+$lt`("sendDate", Calendar.getInstance().let {
                    it.timeInMillis -= 90L * 24 * 60 * 60 * 1000
                    it.set(Calendar.MILLISECOND, 0)
                    it.set(Calendar.SECOND, 0)
                    it.set(Calendar.MINUTE, 0)
                    it.set(Calendar.HOUR_OF_DAY, 0)
                    it.time
                })
            })
            .projection(doc { })
            .sort(doc { `+`("sendDate", 1) })
//            .limit(25000)
            .forEach { c837 ->
                paya_modified_claims.updateOne(doc {
                    `+`("claimId", c837._id)
                }, doc {
                    `+$setOnInsert` {
                        `+`("_class", "com.bioreference.paya.domain.ModifiedClaim")
                        `+`("claimCollection", claimCollection)
                    }
                    `+$addToSet` {
                        `+`("tags", noEobRef)
                    }
                }, UpdateOptions().upsert(true))
            }
}