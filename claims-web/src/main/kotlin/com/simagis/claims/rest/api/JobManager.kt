package com.simagis.claims.rest.api

import com.mongodb.client.FindIterable
import com.mongodb.client.result.UpdateResult
import org.bson.Document
import java.util.concurrent.Callable
import java.util.concurrent.Future

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/17/2017.
 */
object JobManager {
    private fun Document.toJob(): Job = Job.of(this)

    fun insert(job: Job) {
        ClaimDb.apiJobs.insertOne(job.toDoc())
    }

    fun replace(job: Job): UpdateResult = ClaimDb.apiJobs.replaceOne(Document("_id", job.id), job.toDoc())

    operator fun get(id: String): Job? = find { append("_id", id) }.firstOrNull()?.toJob()

    fun find(status: JobStatus? = null, filter: Document.() -> Unit = {}): FindIterable<Document> = ClaimDb.apiJobs
            .find(Document().apply {
                if (status != null) append("status", status.name)
                filter()
            })

    fun submit(callable: Callable<Unit>): Future<Unit> = ClaimDb.ex1.submit(callable)
}

