package com.simagis.claims.rest.api.jobs

import com.simagis.claims.rest.api.*
import org.bson.Document
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
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
        error: JsonObject? = null,
        private var onDone: ((Import) -> Unit)? = null
) : Job(id, created, status, error) {

    private class Running(
            val id: String,
            var future: Future<Unit>? = null,
            var process: JavaProcess? = null
    ) {
        private val isKillingA = AtomicBoolean(false)
        private val isAliveA = AtomicBoolean(false)
        val isAlive: Boolean get() = isAliveA.get()
        val session = UUID.randomUUID().toString()

        fun start(job: Import) {
            isAliveA.set(true)
            future = JobManager.submit(Callable<Unit> {
                try {
                    while (true) {
                        val process = JavaProcess
                                .start(job.loadCommand().apply {
                                    parameters += "-restartable" to session
                                })
                                .also { process = it }

                        job.status = JobStatus.RUNNING
                        JobManager.replace(job)

                        process.waitFor()
                        val exitValue = process.exitValue()
                        if (exitValue == RESTART_CODE && !isKillingA.get()) {
                            continue
                        }
                        job.status = when (exitValue) {
                            0 -> JobStatus.DONE
                            else -> JobStatus.ERROR
                        }
                        JobManager.replace(job)
                        break
                    }
                } catch(e: Throwable) {
                    job.status = JobStatus.ERROR
                    job.error = e.toErrorJsonObject()
                    JobManager.replace(job)
                } finally {
                    isAliveA.set(false)
                    job.onDone?.let { it(job) }
                }
            })
        }

        fun kill(): Boolean {
            isKillingA.set(true)
            future?.cancel(false)
            process?.destroy()
            future?.get()
            return !isAlive
        }
    }

    companion object {
        val TYPE: String = "Import"
        val RESTART_CODE = 302

        private val running: MutableMap<String, Running> = mutableMapOf()
        private val lock = ReentrantLock()

        fun start(onDone: ((Import) -> Unit)? = null): Import = Import(onDone = onDone).apply {
            val runningNew = lock.withLock {
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