package com.simagis.edi.mongodb

import com.simagis.edi.mdb.`+$lt`
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.util.*

fun main(args: Array<String>) {
    println(doc {
        `+`("noEob", true)
        `+$lt`("sendDate", Calendar.getInstance().let {
            it.timeInMillis -= 90L * 24 * 60 * 60 * 1000
            it.set(Calendar.MILLISECOND, 0)
            it.set(Calendar.SECOND, 0)
            it.set(Calendar.MINUTE, 0)
            it.set(Calendar.HOUR_OF_DAY, 0)
            it.time
        })
    }.toJson(JsonWriterSettings(JsonMode.SHELL)))
}