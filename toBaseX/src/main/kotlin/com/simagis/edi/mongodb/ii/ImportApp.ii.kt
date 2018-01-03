package com.simagis.edi.mongodb.ii

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.model.IndexModel
import com.simagis.edi.basex.get
import com.simagis.edi.mdb._id
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import com.simagis.edi.mdb.get
import com.simagis.edi.mongodb.*
import org.bson.Document
import java.io.*
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import kotlin.math.abs

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 12/19/2017.
 */
fun main(args: Array<String>) {
    ImportJob.open(args)
    info("starting job", detailsJson = ImportJob.jobDoc)
    val logLock = ReentrantLock()
    fun log(message: String) = logLock.withLock { println(message) }
    val session = ImportJob.ii.newSession()
    try {
        session.status = IIStatus.RUNNING
        if (ImportJob.options.scanMode == "R") {
            session.step = "Scanning source files"
            ImportJob.options.sourceDir.walk().forEach { file ->
                if (file.isFile) session.files.registerFile(file)
            }
        }

        session.step = "Finding new files"
        session.files.find(IIStatus.NEW).toList().also { files ->
            session.step = "Importing new files"
            ResourceManager().use { executor ->
                files.forEachIndexed { index, file ->
                    executor.call(ImportFileCommand(file)) { result ->
                        val message = """${result.javaClass.simpleName} "${file.doc["names"]}" $index [${files.size}]"""
                        when (result) {
                            is CommandSuccess -> {
                                val info: Document? = result.doc["info"] as? Document
                                log("$message: $info")
                                file.markSucceed(info)
                            }
                            is CommandError -> {
                                val error: Document = result.error
                                log("$message: ${error["message"]}")
                                file.markFailed(error)
                            }
                        }
                    }
                }
            }
        }

        session.step = "Adding new claims"
        ImportJob.ii.getClaims().also { claims: IIClaims ->
            AllClaimsUpdateChannel(ImportJob.options.after).also { channel ->
                claims.findNew().forEach { channel.put(it) }
                channel.shutdownAndWait()
                claims.commit()
            }
        }

        session.step = "Closing session"
        session.status = IIStatus.SUCCESS
    } catch (e: Throwable) {
        session.status = IIStatus.FAILURE
        session.error = e
        e.printStackTrace()
    }
}

private interface Command {
    val memSize: Long
    val command: String
    val doc: Document?
}

private interface SessionCommand : Command {
    val sessionId: Long
}

private class ExitCommand : Command {
    override val command: String = "exit"
    override val memSize: Long = 0
    override val doc: Document? = null
}

private class ImportFileCommand(file: IIFile) : SessionCommand {
    override val command: String = "importFile"
    override val memSize: Long = 128.mb + 1.gb + file.size * 14
    override val doc: Document = file.doc
    override val sessionId: Long = file.sessionId
}

private val Int.mb: Long
    get() = this * 1024L * 1024L

private val Int.gb: Long
    get() = this.mb * 1024L

private sealed class CommandResult
private class CommandSuccess(val doc: Document) : CommandResult()
private abstract class CommandError(val error: Document) : CommandResult()
private class CommandFailure(error: Document) : CommandError(error)
private class ExecutorError(error: Document) : CommandError(error)

private class ResourceManager(sharedMemory: Long = 16.gb) : Closeable {
    private val sharedProcessors = Runtime.getRuntime().availableProcessors()
    private val exeMonitor = java.lang.Object()
    private var exeActive: Executor? = null
    private val executors: List<Executor> by lazy { List(sharedProcessors) { Executor(it) } }
    private val resMonitor = java.lang.Object()
    private val res = Res(sharedMemory, sharedProcessors)
    private var closing = false

    private class Res(var memory: Long, var processors: Int)
    private class Task(val command: Command, val onResult: (CommandResult) -> Unit)
    private class Executor(index: Int) : Runnable {
        private val cx = CommandExecutor()
        private val thread = Thread(this, "executor thread $index").apply { start() }
        private val monitor = java.lang.Object()
        private var isClosing: Boolean = false
            get() = synchronized(monitor) { field }
            set(value) = synchronized(monitor) { field = value; monitor.notifyAll() }
        private var task: Task? = null

        val isReady: Boolean get() = synchronized(monitor) { task == null }

        fun putTask(task: Task) = synchronized(monitor) {
            while (this.task != null) {
                monitor.wait()
            }
            this.task = task
            monitor.notifyAll()
        }

        private fun takeTask(): Task? = synchronized(monitor) {
            task?.let { return it }
            monitor.wait(5000)
            return this.task
        }

        private fun freeTask() = synchronized(monitor) {
            task = null
            monitor.notifyAll()
        }

        override fun run() {
            cx.use { executor ->
                while (!isClosing) {
                    takeTask()?.run {
                        try {
                            executor.memory = command.memSize
                            executor.call(command, onResult)
                        } finally {
                            freeTask()
                        }
                    }
                }
            }
        }

        fun shutdown() {
            isClosing = true
            thread.join()
        }
    }


    fun call(command: Command, onResult: (CommandResult) -> Unit) {
        takeRes(command.memSize)
        takeExecutor().putTask(Task(command) {
            onResult(it)
            freeRes(command.memSize)
            freeExecutor()
        })
    }


    private fun takeExecutor(): Executor = synchronized(exeMonitor) {
        do {
            exeActive?.let { if (it.isReady) return it }
            for (executor in executors) {
                if (executor.isReady) return executor.also { exeActive = it }
            }
            exeMonitor.wait(5000)
        } while (true)
        throw AssertionError()
    }

    private fun freeExecutor() = synchronized(exeMonitor) {
        exeMonitor.notify()
    }

    private fun takeRes(memSize: Long) = synchronized(resMonitor) {
        do {
            if (res.memory - memSize >= 128.mb && res.processors >= 1) {
                res.memory -= memSize
                res.processors--
                break
            }
            if (res.processors == sharedProcessors) {
                res.memory -= memSize
                res.processors--
                break
            }
            resMonitor.wait(5000)
        } while (true)
    }

    private fun freeRes(memSize: Long) = synchronized(resMonitor) {
        res.memory += memSize
        res.processors++
        resMonitor.notify()
    }

    override fun close() {
        closing = true
        executors.forEach { it.shutdown() }
    }
}

private class CommandExecutor(var memory: Long = 2.gb) : Closeable {

    private var process: CommandProcess? = null
    private var processMemory: Long? = null
    var isCalling = false

    fun call(command: Command, onResult: (CommandResult) -> Unit) {
        isCalling = true
        try {
            var result = call(command)
            if (result is ExecutorError) result = call(command)
            onResult(result)
        } finally {
            isCalling = false
        }
    }

    fun free() {
        closeCommandProcessor()
    }

    private fun call(command: Command): CommandResult = process
            .let {
                it ?: newCommandProcess()
            }
            .let {
                if (abs(processMemory!! - memory) < 1.gb) it else {
                    closeCommandProcessor()
                    newCommandProcess()
                }
            }
            .let {
                try {
                    it.call(command)
                } catch (e: Throwable) {
                    ExecutorError(it.toErrorDoc(e))
                }
            }
            .also {
                if (it is ExecutorError) closeCommandProcessor()
            }

    private fun closeCommandProcessor() {
        process?.let {
            process = null
            processMemory = null
            it.close()
        }
    }

    private fun newCommandProcess(): CommandProcess = CommandProcess(memory)
            .also {
                it.run()
                process = it
                processMemory = memory
            }

    override fun close() {
        process?.close()
    }
}

private class CommandProcess(memory: Long) : Closeable {
    private val xmx = "-Xmx${memory / 1.mb}m"
    private var process: Process? = null
    private lateinit var processReader: BufferedReader
    private lateinit var processWriter: OutputStream
    private lateinit var processErr: File

    private val readerLines = LinkedBlockingQueue<Any>()
    private val readerThread = thread(start = false) {
        do {
            try {
                val line: String? = processReader.readLine()
                if (line == null) {
                    readerLines.put(false)
                    break
                } else {
                    readerLines.put(line)
                }
            } catch (e: InterruptedException) {
                break
            }
        } while (!Thread.currentThread().isInterrupted)
    }

    fun run() {
        processErr = File.createTempFile(CommandProcess::javaClass.name + ".", ".err")
        ProcessBuilder()
                .apply {
                    val command = command()
                    command += javaFile.absolutePath
                    command += xmx
                    command += "-cp"
                    command += classPath
                    command += className
                    command += commandLine
                    redirectError(ProcessBuilder.Redirect.to(processErr))
                }
                .also {
                    log.info("> java $xmx $className $commandLine")
                }
                .start()
                .also {
                    process = it
                    processReader = it.inputStream.bufferedReader()
                    processWriter = it.outputStream
                    readerThread.start()
                }
    }

    fun call(command: Command): CommandResult {
        val json: String = Document("command", command.command)
                .also { document ->
                    command.doc?.let { document["doc"] = it }
                    if (command is SessionCommand)
                        document["session"] = command.sessionId
                }
                .toJson()
        val commandLine = json + lineSeparator
        try {
            processWriter.write(commandLine.toByteArray())
            processWriter.flush()
        } catch (e: Throwable) {
            if (command.command == "exit" && process?.isAlive == false) {
                return CommandSuccess(doc { })
            }
            process?.let {
                if (!it.isAlive) return ExecutorError(toErrorDoc(IOException("invalid process status")))
            }
            log.log(Level.WARNING, "error on $commandLine", e)
            throw e
        }

        var item: Any?
        do {
            item = readerLines.poll(5, TimeUnit.SECONDS)
            if (item is String) break
            if (item == true) break
            process?.let {
                if (!it.isAlive) return ExecutorError(toErrorDoc(IOException("invalid process status")))
            }

        } while (true)

        val line = item as? String
        return when {
            line == null -> ExecutorError(toErrorDoc(IOException("invalid process output: $line")))
            line.startsWith("{") -> CommandSuccess(Document.parse(line))
            line.startsWith("error:") -> CommandFailure(Document.parse(line.removePrefix("error:")))
            else -> ExecutorError(toErrorDoc(IOException("invalid process output: $line")))
        }
    }

    override fun close() {
        log.info("exiting $className")
        call(ExitCommand())
        process?.let {
            it.waitFor(30, TimeUnit.SECONDS)
            it.destroy()
        }
        processErr.delete()
    }

    companion object {
        val log: Logger = Logger.getLogger(CommandExecutor::javaClass.name)
        val className = "com.simagis.edi.mongodb.ii.ImportFilesKt"
        val javaHome: File = File(System.getProperty("java.home"))
        val lineSeparator: String = System.getProperty("line.separator")
        val javaFile: File by lazy {
            val bin = javaHome.resolve("bin")
            val java = bin.resolve("java")
            if (java.isFile) java else bin.resolve("java.exe")
        }
        val classPath: String by lazy {
            val pathSeparator: Char = System.getProperty("path.separator").first()
            val debugJars = setOf("idea_rt.jar", "debugger-agent.jar")
            System.getProperty("java.class.path")
                    .split(pathSeparator)
                    .map { File(it) }
                    .filterNot { it.name in debugJars }
                    .filterNot { it.path.startsWith(javaHome.path) }
                    .map { it.path }
                    .joinToString(separator = pathSeparator.toString())
        }
        val commandLine: List<String> by lazy {
            mutableListOf<String>().also { list ->
                listOf("host", "job").forEach { name ->
                    ImportJob.commandLine[name]?.let {
                        list += "-$name"
                        list += it
                    }
                }
            }
        }
    }

    fun toErrorDoc(e: Throwable): Document = e.toErrorDoc().apply {
        val err = try {
            processErr.readText().let { if (it.isNotBlank()) it else null }
        } catch (e: Throwable) {
            null
        }
        err?.let { `+`("err", it) }
    }
}

private sealed class ClaimsUpdateChannel {
    abstract fun put(claim: IIClaim)
    abstract fun shutdownAndWait()
}

private class AllClaimsUpdateChannel(dateAfter: Date?) : ClaimsUpdateChannel() {
    private val channel835 = Claims835UpdateChannel(dateAfter)
    private val channel835a = AClaims835UpdateChannel()
    private val channel837 = Claims837UpdateChannel(dateAfter)
    private val channel837a = AClaims837UpdateChannel()

    override fun put(claim: IIClaim) {
        if (claim.valid) {
            when (claim.type) {
                "835" -> {
                    channel835.put(claim)
                    channel835a.put(claim)
                }
                "837" -> {
                    channel837.put(claim)
                    channel837a.put(claim)
                }
            }
        }
    }

    override fun shutdownAndWait() {
        channel835.shutdownAndWait()
        channel835a.shutdownAndWait()
        channel837.shutdownAndWait()
        channel837a.shutdownAndWait()
    }
}

private abstract class ClaimsUpdateByMaxDateChannel : ClaimsUpdateChannel(), Runnable {
    private val queue: BlockingQueue<IIClaim> = LinkedBlockingQueue(10)
    private val thread = Thread(this, "").apply { start() }

    private object ShutdownMarker : IIClaim

    override fun run() {
        val claimsCollection = openClaimsCollection()
        var lastClaimId: String? = null
        var maxDateClaim: IIClaim? = null
        while (true) {
            val claim = queue.poll(30, TimeUnit.SECONDS) ?: continue
            if (claim == ShutdownMarker) break
            try {
                val claimId = claim.claim["_id"] as String
                when {
                    lastClaimId == null -> {
                        lastClaimId = claimId
                        maxDateClaim = claim
                    }
                    lastClaimId == claimId -> {
                        if (maxDateClaim == null || claim.date > maxDateClaim.date)
                            maxDateClaim = claim

                    }
                    lastClaimId != claimId -> {
                        maxDateClaim?.insert(claimsCollection)
                        lastClaimId = claimId
                        maxDateClaim = claim
                    }
                }
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }
    }

    protected abstract val type: String
    protected abstract fun openClaimsCollection(): DocumentCollection
    protected abstract fun Document.augment(): Document
    protected abstract val dateAfter: Date?

    protected fun DocumentCollection.indexed(): DocumentCollection = apply {
        val indexes: List<IndexModel> = (Document.parse(CreateIndexesJson[type])["indexes"] as? List<*>)
                ?.filterIsInstance<Document>()
                ?.map { IndexModel(it) }
                ?: emptyList()
        createIndexes(indexes)
    }

    private fun IIClaim.insert(claimsCollection: DocumentCollection) {
        val claim = claim.augment()
        try {
            claimsCollection.insertOne(claim)
        } catch (e: MongoWriteException) {
            if (ErrorCategory.fromErrorCode(e.code) != ErrorCategory.DUPLICATE_KEY) throw e
            val key = doc(claim._id)
            val oldDate = claimsCollection.find(key).first()?.let { date(it) }
            val isReplaceRequired = when {
                oldDate == null -> true
                oldDate < date -> true
                else -> false
            }
            if (isReplaceRequired) {
                claimsCollection.findOneAndReplace(key, claim)
            }
        }
    }

    override fun put(claim: IIClaim) {
        if (dateAfter == null || claim.date >= dateAfter) {
            queue.put(claim)
        }
    }

    override fun shutdownAndWait() {
        queue.put(ShutdownMarker)
        thread.join(30_000)
        //TODO add warning if thread.isAlive
    }

}

private class Claims835UpdateChannel(override val dateAfter: Date?) : ClaimsUpdateByMaxDateChannel() {
    override val type: String = "835"
    override fun openClaimsCollection(): DocumentCollection = ImportJob.ii.claims.current.openDb()["claims_$type"].indexed()
    override fun Document.augment(): Document = apply { augment835() }

}

private class AClaims835UpdateChannel : ClaimsUpdateByMaxDateChannel() {
    override val type: String = "835"
    override fun openClaimsCollection(): DocumentCollection = ImportJob.ii.claims.archive.openDb()["claims_${type}a"].indexed()
    override fun Document.augment(): Document = apply { augment835() }
    override val dateAfter: Date? = null
}

private class Claims837UpdateChannel(override val dateAfter: Date?) : ClaimsUpdateByMaxDateChannel() {
    override val type: String = "837"
    override fun openClaimsCollection(): DocumentCollection = ImportJob.ii.claims.current.openDb()["claims_$type"].indexed()
    override fun Document.augment(): Document = apply { augment837() }
}

private class AClaims837UpdateChannel : ClaimsUpdateByMaxDateChannel() {
    override val type: String = "837"
    override fun openClaimsCollection(): DocumentCollection = ImportJob.ii.claims.archive.openDb()["claims_${type}a"].indexed()
    override fun Document.augment(): Document = apply { augment837() }
    override val dateAfter: Date? = null
}
