package com.simagis.claims.rest.api.jobs

import com.simagis.claims.rest.api.*
import com.simagis.edi.mdb.doc
import org.bson.Document
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/17/2017.
 */
class Import private constructor(
        id: String = UUID.randomUUID().toString(),
        created: Date = Date(),
        status: RJobStatus = RJobStatus.NEW,
        options: Document = doc {},
        error: Document? = null,
        private var onDone: ((Import) -> Unit)? = null
) : RJob(id, created, status, options, error) {

    private class Running(
            val id: String,
            var future: Future<Unit>? = null,
            var process: JavaProcess? = null
    ) {
        private val isKillingA = AtomicBoolean(false)
        private val isAliveA = AtomicBoolean(false)
        internal val isAlive: Boolean get() = isAliveA.get()

        fun start(job: Import) {
            isAliveA.set(true)
            future = RJobManager.submit(Callable<Unit> {
                try {
                    fun loadCommand(app: String): JavaProcess.Command = JavaProcess.Command.load(app).apply {
                        parameters += "-host" to ClaimDb.mongoHost
                        parameters += "-job" to id
                    }
                    while (true) {
                        val process = JavaProcess
                                .start(loadCommand("Import"))
                                .also { process = it }

                        RJobManager.update(job.apply { status = RJobStatus.RUNNING }, "status")

                        process.waitFor()
                        val exitValue = process.exitValue()
                        if (exitValue == RESTART_CODE && !isKillingA.get()) {
                            continue
                        }
                        if (exitValue != 0) throw ClaimDbApiException("Invalid exitValue: $exitValue")
                        break
                    }

                    val process = JavaProcess
                            .start(loadCommand("Build835c"))
                            .also { process = it }
                    process.waitFor()
                    val exitValue = process.exitValue()
                    if (exitValue != 0) throw ClaimDbApiException("Invalid exitValue: $exitValue")
                    RJobManager.update(job.apply { status = RJobStatus.DONE }, "status")
                } catch(e: Throwable) {
                    RJobManager.update(job.apply {
                        status = RJobStatus.ERROR
                        error = doc { appendError(e) }
                    }, "status", "error")
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
            future?.let { if (!it.isCancelled) it.get() }
            return !isAlive
        }
    }

    override fun kill(): Boolean = lock.withLock { running[id] }?.kill()
            ?: throw ClaimDbApiException("The job $id isn't running")

    companion object {
        val TYPE: String = "Import"
        val RESTART_CODE = 302

        private val running: MutableMap<String, Running> = mutableMapOf()
        private val lock = ReentrantLock()

        fun start(options: Document = doc {}, onDone: ((Import) -> Unit)? = null): Import = Import(
                options = options,
                onDone = onDone).apply {
            val runningNew = lock.withLock {
                running.values.forEach {
                    if (it.isAlive) {
                        throw ClaimDbApiException("Import already running by job ${it.id}")
                    }
                }
                Running(id).apply {
                    running[id] = this
                }
            }
            RJobManager.insert(this)
            runningNew.start(this)
        }

        inline fun <reified T> Document.req(key: String): T
                = get(key) as? T ?: throw ClaimDbApiException("Invalid job format: $key is not ${T::class}")

        fun of(document: Document): Import = Import(
                id = document.req("_id"),
                created = document.req("created"),
                status = enumValueOf(document.req("status")),
                options = document["options"] as? Document ?: doc {},
                error = document["error"] as? Document
        )
    }
}