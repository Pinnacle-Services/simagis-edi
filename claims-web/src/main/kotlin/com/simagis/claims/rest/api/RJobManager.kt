package com.simagis.claims.rest.api

import com.mongodb.client.FindIterable
import com.simagis.edi.mdb.`+$set`
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import java.util.concurrent.Callable
import java.util.concurrent.Future

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/17/2017.
 */
object RJobManager {
    private fun Document.toJob(): RJob = RJob.of(this)

    fun insert(job: RJob) {
        ClaimDb.apiJobs.insertOne(job.toDoc())
    }

    operator fun get(id: String): RJob? = find { append("_id", id) }.firstOrNull()?.toJob()

    fun find(status: RJobStatus? = null, filter: Document.() -> Unit = {}): FindIterable<Document> = ClaimDb.apiJobs
            .find(Document().apply {
                if (status != null) append("status", status.name)
                filter()
            })

    fun submit(callable: Callable<Unit>): Future<Unit> = ClaimDb.ex1.submit(callable)

    fun update(job: RJob, vararg names: String) {
        if (names.isNotEmpty()) {
            ClaimDb.apiJobs.updateOne(doc(job.id), doc {
                `+$set` {
                    names.forEach { name ->
                        `+`(name, when (name) {
                            "created" -> job.created
                            "status" -> job.status.name
                            "options" -> job.options
                            "error" -> job.error
                            else -> null
                        })
                    }
                }
            })
        }
    }
}

