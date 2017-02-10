package com.simagis.edi.basex

import com.berryworks.edireader.EDISyntaxException
import org.basex.core.cmd.Add
import org.basex.core.cmd.InfoDB
import org.basex.core.cmd.Optimize
import java.io.File
import java.util.*

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/9/2017.
 */

fun main(args: Array<String>) {
    val defaultBaseXDataDir = java.io.File(System.getenv("USERPROFILE") ?: ".").resolve("BaseXData")
    val defaultSession = UUID.randomUUID().toString()
    val defaultPrefix = "isa-doc-"
    val commandLine = com.berryworks.edireader.util.CommandLine(args)
    if (commandLine["?"] != null) {
        exit("""Usage: ToBaseXReg [-s session] [-p prefix] [-o outputDir] [-x dataDir] [-L LIMIT]
        auto session = $defaultSession
        auto prefix  = $defaultSession-$defaultPrefix
        default dataDir = $defaultBaseXDataDir
""")
    }
    val session = commandLine["s"] ?: defaultSession
    val dataDir = commandLine["x"]?.let(::File) ?: defaultBaseXDataDir
    val outputDir = commandLine["o"]?.let(::File)?.exitIfNotIsDir("outputDir")
    val prefix = commandLine["p"] ?: "$defaultSession-$defaultPrefix"
    val limit = commandLine["L"]?.let(String::toLong) ?: Long.MAX_VALUE

    var limitCount = 0L
    val newNames = mutableListOf<String>()

    fun XReg.XSession.invalidISA(isa: ISA, e: Exception? = null) = xLog.warning(
            "Invalid ISA: ${isa.stat}",
            e,
            details = isa.code,
            detailsXml = if (e is EDISyntaxException) try {
                isa.toXML().toString(ISA.CHARSET)
            } catch(e: Exception) {
                null
            } else null
    )

    XReg().newSession(session) {
        fun DBX.add(path: String, isa: ISA): Boolean {
            xLog.trace("$path: ${isa.stat} at ${isa.position}")
            if (isa.valid) try {
                on(prefix + isa.name) { context ->
                    with(Add(path)) {
                        setInput(isa.toXML().inputStream())
                        execute(context)
                    }
                }
                return true
            } catch(e: Exception) {
                invalidISA(isa, e)
            } else {
                invalidISA(isa, null)
            }
            return false
        }

        DBX().use { dbx ->

            forEachInputFile { file ->
                withFile(file) {
                    if (fileStatus == "") {
                        var validISA = 0
                        var invalidISA = 0
                        val isaList: List<ISA> = try {
                            split()
                        } catch(e: Exception) {
                            xLog.warning("error on split($asFile)", e)
                            updateFileStatus("INVALID")
                            return@withFile
                        }
                        isaList.forEachIndexed { i, isa ->
                            withISA(isa) {
                                if (isaStatus == "") {
                                    if (limitCount++ < limit) {
                                        if (dbx.add("${file.name}.part-${i + 1}(${isaList.size}).xml", isa)) {
                                            validISA++
                                            updateIsaStatus("LOCAL")
                                        } else {
                                            invalidISA++
                                            updateIsaStatus("INVALID")
                                        }
                                    }
                                }
                            }
                        }
                        updateFileStatus("REGISTERED")
                        if (invalidISA != 0) {
                            xLog.warning("$invalidISA invalid ISA(s) in file: $file")
                        }
                        xLog.info("File In Local: $file")
                    }
                }
            }

            dbx.names.forEach { name ->
                newNames += name
                dbx.on(name) { context ->
                    xLog.info("> Database $name: Optimization...")
                    Optimize().execute(context)
                    xLog.info("> Database $name: Information:", details = InfoDB().execute(context))
                }
            }
        }

        if (outputDir != null) {
            newNames.forEach { name ->
                val newDataDir = dataDir.resolve(name)
                val outDataDir = outputDir.resolve(name)
                if (outDataDir.exists()) {
                    xLog.warning("Database ${outDataDir.absolutePath} already exists")
                } else {
                    if (!newDataDir.renameTo(outDataDir)) {
                        xLog.warning("Unable to rename ${newDataDir.absolutePath} to ${outDataDir.absolutePath}")
                    }
                }
            }
        }
    }
}