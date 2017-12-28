package com.simagis.edi.mongodb.ii

import com.simagis.edi.basex.get
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import com.simagis.edi.mongodb.ImportJob
import com.simagis.edi.mongodb.ImportJob.ii.Status.*
import com.simagis.edi.mongodb.info
import org.bson.Document
import java.io.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.concurrent.thread
import kotlin.math.abs

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 12/19/2017.
 */
fun main(args: Array<String>) {
    ImportJob.open(args)
    info("starting job", detailsJson = ImportJob.jobDoc)
    val session = ImportJob.ii.newSession()
    try {
        session.status = RUNNING
        if (ImportJob.options.scanMode == "R") {
            ImportJob.options.sourceDir.walk().forEach { file ->
                if (file.isFile) session.files.registerFile(file)
            }
        }

        ResourceManager().use { executor ->
            for (file: ImportJob.ii.File in session.files.find(NEW)) {
                executor.call(ImportFileCommand(file)) { result ->
                    print(result.javaClass.simpleName)
                    when (result) {
                        is CommandSuccess -> {
                            val info: Document? = result.doc["info"] as? Document
                            println(": $info")
                            file.markSucceed(info)
                        }
                        is CommandError -> {
                            println(result.error["message"])
                            file.markFailed(result.error)
                        }
                    }
                }
            }
        }

        session.status = SUCCESS
    } catch (e: Throwable) {
        session.status = FAILURE
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

private class ImportFileCommand(file: ImportJob.ii.File) : SessionCommand {
    override val command: String = "importFile"
    override val memSize: Long = 128.mb + file.size * 4
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

private class ResourceManager(private val sharedMemory: Long = 16.gb) : Closeable {
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
            set(value) = synchronized(monitor) { field = value }
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
            exeMonitor.wait()
        } while (true)
        throw AssertionError()
    }

    private fun freeExecutor() = synchronized(exeMonitor) {
        exeMonitor.notify()
    }

    private fun takeRes(memSize: Long) = synchronized(resMonitor) {
        do {
            if (res.processors == sharedProcessors) {
                res.memory -= memSize
                res.processors--
                break
            }
            if (res.memory + memSize >= sharedMemory) {
                res.memory -= memSize
                res.processors--
                break
            }
            resMonitor.wait()
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

private class CommandExecutor(var memory: Long = 16.gb) : Closeable {

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

private class CommandProcess(private val memory: Long) : Closeable {
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
                    command += "-cp"
                    command += classPath
                    command += className
                    command += commandLine
                    redirectError(ProcessBuilder.Redirect.to(processErr))
                }
                .also {
                    log.info("starting $className $memory")
                    log.log(Level.FINEST, "starting ${it.command().joinToString(separator = " ") { """"$it"""" }}")
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

