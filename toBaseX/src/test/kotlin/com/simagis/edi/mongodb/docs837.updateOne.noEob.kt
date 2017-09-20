package com.simagis.edi.mongodb

import com.simagis.edi.mdb.`+$set`
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc

fun main(args: Array<String>) {
    ImportJob.open(args)
    val docs837 = ImportJob.dbs["claims"].getCollection("claims_837")
    docs837.updateOne(doc("MEJ1049749619-C-1"), doc {
        `+$set` {
            `+`("eobSize", 1)
            `+`("noEob", false)
        }
    })
}