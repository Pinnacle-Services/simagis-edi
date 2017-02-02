package com.simagis.edi.basex

import com.berryworks.edireader.EDISyntaxException
import org.basex.core.cmd.Add
import org.basex.core.cmd.InfoDB
import org.basex.core.cmd.Optimize
import java.io.File
import java.io.FileFilter
import kotlin.system.exitProcess


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/1/2017.
 */

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        exit("Usage: ToBaseX <inputDir> [L:LIMIT]")
    }
    val inputDir = File(args[0]).apply { if (!isDirectory) exit("Invalid inputDir: $absolutePath") }
    var limit: Long = args.find { it.startsWith("L:") }?.substring(2)?.toLong() ?: Long.MAX_VALUE

    DBX().use { dbx ->
        inputDir.listFiles(FileFilter { it.isFile })?.let files@ { files ->
            files.forEach { file ->
                try {
                    if (file.name.toLowerCase().endsWith(".xml")) {
                        if (limit-- <= 0L) return@files
                        dbx.onCollection("any-xml") { context ->
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

        dbx.collections.forEach { collection ->
            dbx.onCollection(collection) { context ->
                println("* Collection $collection")
                println("optimization...")
                Optimize().execute(context)
                println("Information:")
                println(InfoDB().execute(context))
            }
        }
    }
}

private fun DBX.add(path: String, isa: ISA) {
    println("$path: ${isa.stat} at ${isa.position}")
    if (isa.valid) {
        try {
            onCollection("isa-doc-${isa.stat.doc.type}") { context ->
                with(Add(path)) {
                    setInput(isa.toXML().inputStream())
                    execute(context)
                }
            }
        } catch(e: Exception) {
            e.printStackTrace()
            println("ISA: " + isa.code)
            if (e !is EDISyntaxException) {
                println("XML: " + isa.toXML().toString(ISA.XML_CHARSET))
            }
        }
    } else {
        println(isa.code)
    }
}

fun warning(message: String) {
    println("WARNING: " + message)
}

private fun exit(message: String) {
    println(message)
    exitProcess(1)
}