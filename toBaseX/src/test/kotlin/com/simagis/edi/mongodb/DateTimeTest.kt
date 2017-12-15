package com.simagis.edi.mongodb

import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import java.time.Instant
import java.util.*


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 12/15/2017.
 */
fun main(args: Array<String>) {
    val instant = Instant.parse("2017-01-01T12:00:00.000Z")
    val date = Date.from(instant)
    println(instant)
    println(date)

    ImportJob.open(args)
    val dateTimeTest = ImportJob.dbs["debug"].getCollection("dateTimeTest")
    dateTimeTest.insertOne(doc {
        `+`("date", date)
    })
}
