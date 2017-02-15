package com.simagis.edi.basex

import com.berryworks.edireader.EDISyntaxException
import com.berryworks.edireader.util.CommandLine
import org.basex.core.cmd.Add
import org.basex.core.cmd.InfoDB
import org.basex.core.cmd.Optimize
import java.io.File
import java.io.FileFilter


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/1/2017.
 */

fun main(args: Array<String>) {
    val defaultBaseXDataDir = File(System.getenv("USERPROFILE") ?: ".").resolve("BaseXData")
    val defaultPrefix = "isa-doc-"
    val commandLine = CommandLine(args)
    if (commandLine.size() == 0) {
        exit("""Usage: ToBaseX <inputDir> [-x dataDir] [-o outputDir] [-p prefix] [-L LIMIT]
        default dataDir = $defaultBaseXDataDir
        default prefix = $defaultPrefix
""")
    }
    val inputDir = commandLine[0]?.let(::File)!!.exitIfNotIsDir("inputDir")
    val dataDir = commandLine["x"]?.let(::File) ?: defaultBaseXDataDir
    val outputDir = commandLine["o"]?.let(::File)?.exitIfNotIsDir("outputDir")
    val prefix = commandLine["p"] ?: defaultPrefix
    var limit = commandLine["L"]?.let(String::toLong) ?: Long.MAX_VALUE
    val newNames = mutableListOf<String>()

    fun DBX.add(path: String, isa: ISA) {
        println("$path: ${isa.stat} at ${isa.position}")
        if (isa.valid) {
            try {
                on(prefix + isa.dbName) { context ->
                    with(Add(path)) {
                        setInput(isa.toXML().inputStream())
                        execute(context)
                    }
                }
            } catch(e: Exception) {
                e.printStackTrace()
                println("ISA: " + isa.code)
                if (e !is EDISyntaxException) {
                    println("XML: " + isa.toXML().toString(ISA.CHARSET))
                }
            }
        } else {
            println(isa.code)
        }
    }

    DBX().use { dbx ->
        inputDir.listFiles(FileFilter { it.isFile })?.let files@ { files ->
            files.forEach { file ->
                try {
                    if (file.name.toLowerCase().endsWith(".xml")) {
                        if (limit-- <= 0L) return@files
                        dbx.on("${prefix}any-xml") { context ->
                            Add(file.name, file.canonicalPath).execute(context)
                        }
                    } else {
                        val isaList = ISA.read(file)
                        when {
                            isaList.isEmpty() -> warning("ISA not found in $file")
                            isaList.size == 1 -> isaList.first().let { isa ->
                                if (isa.valid && limit-- <= 0L) return@files
                                dbx.add("${file.name}.xml", isa)
                            }
                            isaList.size > 1 -> isaList.forEachIndexed { i, isa ->
                                if (isa.valid && limit-- <= 0L) return@files
                                dbx.add("${file.name}.part-${i + 1}(${isaList.size}).xml", isa)
                            }
                        }
                    }
                } catch (e: Exception) {
                    println("Invalid ${file.name}")
                    e.printStackTrace()
                }
            }
        }

        dbx.names.forEach { name ->
            newNames += name
            dbx.on(name) { context ->
                println("* Database $name")
                println("> Optimization...")
                Optimize().execute(context)
                println("# Information:")
                println(InfoDB().execute(context))
            }
        }
    }

    outputDir?.let { outputDir ->
        newNames.forEach { name ->
            val newDataDir = dataDir.resolve(name)
            val outDataDir = outputDir.resolve(name)
            if (outDataDir.exists()) {
                warning("Database ${outDataDir.absolutePath} already exists")
            } else {
                if (!newDataDir.renameTo(outDataDir)) {
                    warning("Unable to rename ${newDataDir.absolutePath} to ${outDataDir.absolutePath}")
                }
            }
        }
    }
}