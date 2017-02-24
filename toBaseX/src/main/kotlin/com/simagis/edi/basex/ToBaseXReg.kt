package com.simagis.edi.basex

import com.berryworks.edireader.EDISyntaxException
import org.basex.core.cmd.InfoDB
import org.basex.core.cmd.Optimize
import org.basex.core.cmd.Replace
import java.util.*
import kotlin.system.exitProcess

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/9/2017.
 */

fun main(args: Array<String>) {
    val defaultSession = UUID.randomUUID().toString()
    val commandLine = com.berryworks.edireader.util.CommandLine(args)
    if (commandLine["?"] != null) {
        exit("""Usage: ToBaseXReg [-s session] [-L LIMIT]
        auto session = $defaultSession
""")
    }
    val session = commandLine["s"] ?: defaultSession
    val limit = commandLine["L"]?.let(String::toLong) ?: Long.MAX_VALUE
    var limitCount = 0L
    var exitCode = 0

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
        fun DBX.replace(path: String, isa: ISA): Boolean {
            xLog.trace("replacing ISA $path: ${isa.stat} at ${isa.position}")
            if (isa.valid) try {
                on("isa-doc-${isa.dbName}") { context ->
                    with(Replace(path)) {
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

        xLog.info("session: $session")

        DBX().use { dbx ->
            try {
                status = "SCANNING"
                val inputFiles = listInputFiles()
                filesIn = inputFiles.size
                status = "PROCESSING"

                inputFiles.forEach { file ->
                    withFile(file) {
                        if (fileStatus == "") {
                            var validISA = 0
                            var invalidISA = 0
                            val isaList: List<ISA> = try {
                                split()
                            } catch(e: Exception) {
                                xLog.warning("File $file parsing error", e)
                                fileStatus = "INVALID"
                                return@withFile
                            }
                            isaList.forEach { isa ->
                                withISA(isa) {
                                    if (isaStatus == "") {
                                        if (limitCount++ < limit) {
                                            if (dbx.replace("$isaDigest.xml", isa)) {
                                                validISA++
                                                isaStatus = "BASE"
                                            } else {
                                                invalidISA++
                                                isaStatus = "INVALID"
                                            }
                                        }
                                    }
                                }
                            }
                            fileStatus = "REGISTERED"
                            if (invalidISA != 0) {
                                xLog.warning("$invalidISA invalid ISA(s) in file: $file")
                            }
                            xLog.info("File $file is registered with $validISA new valid ISA(s) of ${isaList.size}")
                        }
                    }
                    filesDone++
                }
            } catch(e: Throwable) {
                xLog.error("File processing error", e)
                exitCode = 2
            }

            status = "OPTIMIZATION"
            try {
                optimizationsIn = dbx.names.size
                dbx.names.forEach { name ->
                    dbx.on(name) { context ->
                        xLog.info("> Database $name: Optimization...")
                        Optimize().execute(context)
                        xLog.info("> Database $name: Information:", details = InfoDB().execute(context))
                    }
                    optimizationsDone++
                }
            } catch(e: Throwable) {
                xLog.error("Database optimization error", e)
                exitCode = 3
            }
        }
    }

    exitProcess(exitCode)
}