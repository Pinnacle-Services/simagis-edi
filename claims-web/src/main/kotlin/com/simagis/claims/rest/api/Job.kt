package com.simagis.claims.rest.api

import com.simagis.claims.rest.api.jobs.Import
import org.bson.Document
import java.text.SimpleDateFormat
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonObjectBuilder

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/17/2017.
 */
abstract class Job(
        val id: String = UUID.randomUUID().toString(),
        val created: Date = Date(),
        var status: JobStatus = JobStatus.NEW,
        var error: JsonObject? = null
) {
    val type: String get() = this.javaClass.simpleName


    open fun toDoc(): Document = Document("_id", id).apply {
        append("type", type)
        append("created", created)
        append("status", status.name)
        error?.let { append("error", it.toDocument()) }
    }

    open fun toJson(): JsonObjectBuilder = Json.createObjectBuilder().apply {
        add("id", id)
        add("type", type)
        add("created", timeFormat.format(created))
        add("status", status.name)
        error?.let { add("error", it) }

    }

    abstract fun kill(): Boolean

    companion object {
        fun of(document: Document): Job {
            val type = document.getString("type")
            return when (type) {
                Import.TYPE -> Import.of(document)
                else -> throw ClaimDbApiException("Invalid Job type: $type")
            }
        }

        val timeFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z")
    }
}

