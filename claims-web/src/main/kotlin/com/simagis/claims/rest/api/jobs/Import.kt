package com.simagis.claims.rest.api.jobs

import com.simagis.claims.rest.api.*
import org.bson.Document
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import javax.json.JsonObject
import kotlin.concurrent.withLock

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/17/2017.
 */
class Import private constructor(
        id: String = UUID.randomUUID().toString(),
        created: Date = Date(),
        status: JobStatus = JobStatus.NEW,
        error: JsonObject? = null
) : Job(id, created, status, error) {

    private class Running(
            val id: String,
            var future: Future<Unit>? = null,
            var process: JavaProcess? = null
    ) {
        val isAlive: Boolean get() = process?.isAlive == true

        fun start(job: Import) {
            future = JobManager.submit(Callable<Unit> {
                try {
                    JavaProcess.start(job.loadCommand()).use {
                        process = it

                        job.status = JobStatus.RUNNING
                        JobManager.replace(job)

                        it.waitFor()
                        job.status = when (it.exitValue()) {
                            0 -> JobStatus.DONE
                            else -> JobStatus.ERROR
                        }
                        JobManager.replace(job)
                    }
                } catch(e: Throwable) {
                    job.status = JobStatus.ERROR
                    job.error = e.toErrorJsonObject()
                    JobManager.replace(job)
                }
            })
        }

        fun kill(): Boolean {
            future?.cancel(false)
            return process?.let {
                if (it.isAlive) {
                    it.destroy()
                    it.waitFor(10L, TimeUnit.SECONDS)
                    if (it.isAlive) it.destroyForcibly()
                }
                !it.isAlive
            } ?: false
        }
    }

    companion object {
        val TYPE: String = "Import"

        private val running: MutableMap<String, Running> = mutableMapOf()
        private val lock = ReentrantLock()

        fun start(): Import = Import().apply {
            val runningNew = Companion.lock.withLock {
                Companion.running.values.forEach {
                    if (it.isAlive) {
                        throw ClaimDbApiException("Import already running by job ${it.id}")
                    }
                }
                Running(id).apply {
                    Companion.running[id] = this
                }
            }
            JobManager.insert(this)
            runningNew.start(this)
        }

        inline fun <reified T> Document.req(key: String): T
                = get(key) as? T ?: throw ClaimDbApiException("Invalid job format: $key is not ${T::class}")

        fun of(document: Document): Import = Import(
                id = document.req("_id"),
                created = document.req("created"),
                status = enumValueOf(document.req("status")),
                error = (document["error"] as? Document?)?.toJsonObject()
        )
    }

    override fun kill(): Boolean = Companion.running[id]?.kill() ?: throw ClaimDbApiException("The job $id isn't running")

    private fun loadCommand(): JavaProcess.Command = JavaProcess.Command.load("Import").apply {
        parameters += "-host" to ClaimDb.mongoHost
    }
}