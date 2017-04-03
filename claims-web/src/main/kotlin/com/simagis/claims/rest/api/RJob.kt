package com.simagis.claims.rest.api

import com.simagis.claims.rest.api.jobs.Import
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import java.util.*

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/17/2017.
 */
abstract class RJob(
        val id: String = UUID.randomUUID().toString(),
        val created: Date = Date(),
        var status: RJobStatus = RJobStatus.NEW,
        var options: Document = doc {},
        var error: Document? = null
) {
    val type: String get() = this.javaClass.simpleName


    open fun toDoc(): Document = doc(id).apply {
        `+`("type", type)
        `+`("created", created)
        `+`("status", status.name)
        `+`("options", options)
        error?.get("error")?.let { `+`("error", it) }
    }

    open fun kill(): Boolean = false

    companion object {
        fun of(document: Document): RJob = document["type"].let { type ->
            when (type) {
                Import.TYPE -> Import.of(document)
                else -> throw ClaimDbApiException("Invalid Job type: $type")
            }
        }
    }
}

