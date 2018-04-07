package com.simagis.claims.rest.api

import com.simagis.claims.clientName
import java.io.*
import java.util.concurrent.TimeUnit
import javax.json.Json

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/17/2017.
 */
internal class JavaProcess private constructor(val command: Command) : Closeable {
    var process: Process? = null

    open class Args {
        val values = mutableListOf<String>()
        fun toArray(): Array<String> = values.toTypedArray()
        override fun toString(): String = toArray().joinToString(separator = " ") {
            if (it.contains(' ')) """"$it""" else it
        }

        operator fun plusAssign(value: String) {
            values += value
        }

        operator fun plusAssign(value: Pair<String, String>) {
            values += value.first
            values += value.second
        }
    }

    class Options : Args() {
        init {
            this += "-Dpaypredict.client=$clientName"
        }
    }

    class Parameters : Args()

    data class Command(val options: Options = Options(),
                       val className: String,
                       val parameters: Parameters = Parameters()) {

        fun toArray(): Array<String> = options.toArray() + className + parameters.toArray()

        companion object {
            fun load(name: String): Command {
                val file = claimDbRootDir.resolve("apps").let {
                    val local = it.resolve("$name.local.json")
                    if (local.isFile) local else it.resolve("$name.json")
                }
                return file.reader().use { Json.createReader(it).readObject() }.let { json ->
                    Command(className = json.getString("className")).apply {
                        json.getJsonArray("options")?.let { it.forEach { options += it.toString() } }
                        json.getJsonArray("parameters")?.let { it.forEach { parameters += it.toString() } }
                    }
                }
            }
        }
    }

    companion object {
        private val javaExe: File by lazy {
            claimDbRootDir.resolve("jre").let {
                if (it.isDirectory) it.absoluteFile
                else File(System.getProperty("java.home"))
            }.resolve("bin").resolve("java.exe").absoluteFile
        }

        fun start(command: Command): JavaProcess = JavaProcess(command).start()
    }

    private fun start(): JavaProcess {
        try {
            process = ProcessBuilder()
                    .command(listOf(javaExe.absolutePath, "-cp", "lib/*", *command.toArray()))
                    .directory(claimDbRootDir)
                    .redirectOutput(outFile)
                    .redirectError(errFile)
                    .start()
        } catch(e: Exception) {
            close()
            throw e
        }
        return this
    }

    private val outFile = File.createTempFile("${command.className}.", ".out.txt", claimDbTempDir)
    private val errFile = File.createTempFile("${command.className}.", ".err.txt", claimDbTempDir)

    override fun close() {
        if (!isAlive && exitValue() == 0) {
            outFile.delete()
            errFile.delete()
        }
    }


    val isAlive: Boolean = process?.isAlive ?: false
    fun exitValue(): Int = process?.exitValue() ?: -1
    fun waitFor() = process?.waitFor()
    fun waitFor(l: Long, timeUnit: TimeUnit) = process?.waitFor(l, timeUnit)
    fun destroy() = process?.destroy()
    fun destroyForcibly() = process?.destroyForcibly()

}