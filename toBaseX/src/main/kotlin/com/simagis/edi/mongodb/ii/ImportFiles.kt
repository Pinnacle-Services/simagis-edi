package com.simagis.edi.mongodb.ii

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.simagis.edi.basex.ISA
import com.simagis.edi.mdb.*
import com.simagis.edi.mongodb.DocumentCollection
import com.simagis.edi.mongodb.ImportJob
import com.simagis.edi.mongodb.parseDT8
import com.simagis.edi.mongodb.warning
import org.basex.core.Context
import org.basex.core.MainOptions
import org.basex.core.cmd.CreateDB
import org.basex.core.cmd.Replace
import org.basex.core.cmd.XQuery
import org.bson.Document
import java.io.*
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.logging.Level
import java.util.logging.Logger
import javax.json.Json
import javax.json.JsonArray
import kotlin.concurrent.thread
import kotlin.system.exitProcess

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 12/23/2017.
 */
private class ImportFiles

private val log: Logger = Logger.getLogger(ImportFiles::javaClass.name)

fun main(args: Array<String>) {
    CmdChannel().use { cmdChannel: CmdChannel ->
        try {
            ImportJob.open(args)
            cmdChannel.forEach { cmd ->
                cmd.run()
                if (cmd is ExitCmd) exitProcess(0)
            }
            cmdChannel.printEndMarker()
        } catch (e: Throwable) {
            cmdChannel.printError(e)
            log.log(Level.SEVERE, "Global error", e)
            exitProcess(1)
        }
    }
    exitProcess(0)
}


private val xqTypes: MutableMap<String, String> = ConcurrentHashMap()
private val inMemoryBaseX: ThreadLocal<Context> = ThreadLocal.withInitial {
    Context().apply {
        options.set(MainOptions.MAINMEM, true)
        CreateDB("memory").execute(this)
    }
}

private fun Document.fixTypes() {
    fun String.isTyped() = contains('-')
    fun String.type() = substringAfter('-')
    fun String.name() = substringBefore('-')
    keys.toList().forEach { key ->
        fun append(value: String?, cast: (String) -> Any?) {
            if (value != null && value.isNotBlank())
                append(key.name(), cast(value))
        }

        fun Double.toD0(): Double = if (isFinite()) this else 0.toDouble()

        fun Double.toC0(): Double = if (isFinite()) "%.2f".format(this).toDouble() else 0.toDouble()

        when (key.type()) {
            "I" -> append(getString(key)) { it.toIntOrNull() }
            "I0" -> append(getString(key)) { it.toIntOrNull() ?: 0 }
            "L" -> append(getString(key)) { it.toLongOrNull() }
            "L0" -> append(getString(key)) { it.toLongOrNull() ?: 0.toLong() }

            "D",
            "F" -> append(getString(key)) { it.toDoubleOrNull()?.toD0() }
            "D0",
            "F0" -> append(getString(key)) { it.toDoubleOrNull()?.toD0() ?: 0.0.toD0() }

            "C" -> append(getString(key)) { it.toDoubleOrNull()?.toC0() }
            "C0" -> append(getString(key)) { it.toDoubleOrNull()?.toC0() ?: 0.0.toC0() }

            "CC" -> append(getString(key)) {
                it.split("[\\s,;.]".toRegex())
                        .filter(String::isNotEmpty)
                        .map(String::toLowerCase)
                        .map(String::capitalize)
                        .joinToString(separator = " ")
            }
            "DT8" -> getString(key)?.also { value ->
                parseDT8(value)?.also { append(key.name(), it) }
            }
        }
    }
    keys.removeIf(String::isTyped)
    values.forEach {
        when (it) {
            is Document -> it.fixTypes()
            is List<*> -> it.forEach { (it as? Document)?.fixTypes() }
        }
    }
}

private fun Document.prepare(): Document = apply {
    append("_id", remove("id") ?: throw AssertionError("id not found"))
    fixTypes()
}

private sealed class Cmd(protected val channel: CmdChannel) {
    abstract fun run()
}

private class UnknownCmd(channel: CmdChannel, private val command: String?) : Cmd(channel) {
    override fun run() {
        channel.printError(AssertionError("Unknown command $command"))
    }
}

private class ImportFileCmd(channel: CmdChannel,
                            private val sessionId: Long,
                            private val fileDoc: Document) : Cmd(channel) {
    private val fileId = fileDoc._id as String

    override fun run() {
        val claims: DocumentCollection = ImportJob.ii.sourceClaims.openClaims()
        insertClaims(claims)
        channel.printDoc(fileDoc)
    }

    private fun insertClaims(claims: DocumentCollection) {
        var isaFound = 0
        var claimsFound = 0
        var claimsSucceed = 0

        val file = File((fileDoc["paths"] as List<*>).last() as String)
        ISA.read(file).also { isaFound = it.size }.forEachIndexed { isaIndex, isa ->
            isa.stat.error?.let { throw it }
            isa.toClaimsJsonArray()
                    ?.also { claimsFound += it.size }
                    ?.map { Document.parse(it.toString()) }
                    ?.map { it.prepare() }
                    ?.forEachIndexed { claimIndex, claim ->
                        val claimId = claim.digest()
                        try {
                            claims.insertOne(doc(claimId) {
                                `+`("claim", claim)
                                `+`("type", isa.type)
                                `+`("session", sessionId)
                                `+`("files", listOf(fileId))
                                `+`("pos", doc { `+`(fileId, listOf(isaIndex, claimIndex)) })
                            })
                        } catch (e: MongoWriteException) {
                            if (ErrorCategory.fromErrorCode(e.code) != ErrorCategory.DUPLICATE_KEY) throw e
                            claims.updateOne(doc(claimId), doc {
                                `+$addToSet++` { `+`("files", fileId) }
                                `+$set` {
                                    `+`("session", sessionId)
                                    `+`("pos.$fileId", listOf(isaIndex, claimIndex))
                                }
                            })
                        }
                        claimsSucceed++
                    }
        }
        fileDoc.updateInfo(mapOf(
                "isaFound" to isaFound,
                "claimsFound" to claimsFound,
                "claimsSucceed" to claimsSucceed))
    }
}

private class ExitCmd(channel: CmdChannel) : Cmd(channel) {
    override fun run() {
        channel.printDoc(doc { })
    }
}

private fun Document.updateInfo(counters: Map<String, Int>) {
    val info = this["info"] as? Document ?: Document().also { this["info"] = it }
    counters.forEach { name, value ->
        if (value > 0) info[name] = value else info.remove(name)
    }
    if (info.isEmpty()) this.remove("info")
}

private fun Document.digest(): String {
    val digest: MessageDigest = MessageDigest.getInstance("SHA")
    fun Document.update() {
        fun Any?.toBytes(): ByteArray = when (this) {
            is String -> toByteArray()
            else -> toString().toByteArray()
        }

        fun List<*>.update() {
            forEachIndexed { index, item ->
                digest.update(index.toBytes())
                digest.update(':'.toByte())
                digest.update('['.toByte())
                when (item) {
                    is Document -> item.update()
                    is List<*> -> item.update()
                    else -> digest.update(item.toBytes())
                }
                digest.update(']'.toByte())
            }
        }
        keys.toSortedSet().forEach { key ->
            digest.update(key.toByteArray())
            digest.update(':'.toByte())
            val value = this[key]
            when (value) {
                is Document -> value.update()
                is List<*> -> value.update()
                else -> digest.update(value.toBytes())
            }
        }
    }
    update()
    return digest.digest().toHexStr()
}

private fun ISA.toClaimsJsonArray(): JsonArray? {
    if (valid) try {
        val xqText = stat.doc.type?.let { type ->
            xqTypes.getOrPut(type) {
                ImportJob.options.xqDir.resolve("isa-claims-$type.xq").let { xqFile ->
                    if (xqFile.isFile) xqFile.readText() else {
                        warning("$xqFile not found")
                        ""
                    }
                }
            }
        } ?: ""

        if (xqText.isEmpty()) {
            return null
        }

        val context = inMemoryBaseX.get()

        with(Replace("doc")) {
            setInput(toXML().inputStream())
            execute(context)
        }

        return with(XQuery(xqText)) {
            val json = execute(context)
            Json.createReader(json.reader()).readArray()
        }
    } catch (e: Throwable) {
    } else {
    }
    return null
}

private class CmdChannel : Closeable, Sequence<Cmd> {

    fun printDoc(document: Document) {
        o.println(document.toJson())
    }

    fun printEndMarker() {
        o.println()
    }

    fun printError(error: Throwable) {
        o.println("error:" + error.toDocument().toJson())
    }

    private val i: InputStream = System.`in`
    private val o: PrintStream = System.out

    private val lines = LinkedBlockingQueue<Any>()
    private val iThread = thread(start = false) {
        val reader = i.bufferedReader()
        do {
            try {
                val line: String? = reader.readLine()
                if (line == null || line == ":q") {
                    lines.put(false)
                    break
                } else {
                    lines.put(line)
                }
            } catch (e: InterruptedException) {
                break
            }
        } while (!Thread.currentThread().isInterrupted)
    }

    init {
        System.setOut(PrintStream(object : OutputStream() {
            override fun write(b: Int) {}
        }))
        iThread.start()
    }

    override fun iterator(): Iterator<Cmd> {
        val channel = this
        return object : AbstractIterator<Cmd>() {
            override fun computeNext() {
                val value = lines.take()
                if (value is String) {
                    val commandDoc: Document = Document.parse(value)
                    val command = commandDoc["command"] as? String
                    when (command) {
                        "importFile" -> setNext(ImportFileCmd(
                                channel,
                                commandDoc["session"] as Long,
                                commandDoc["doc"] as Document))
                        "exit" -> setNext(ExitCmd(channel))
                        else -> setNext(UnknownCmd(channel, command))
                    }
                } else done()
            }
        }
    }

    override fun close() {
        iThread.interrupt()
        iThread.join()
    }

    private fun Throwable.toDocument(): Document = doc {
        `+`("class", this@toDocument.javaClass.name)
        `+`("message", message)
        `+`("stackTrace", StringWriter()
                .also { PrintWriter(it).use { printStackTrace(it) } }
                .toString())
    }

}

