package com.simagis.edi.basex

import com.berryworks.edireader.EDISyntaxException
import org.basex.core.Context
import org.basex.core.cmd.Add
import org.basex.core.cmd.CreateDB
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
    if (args.size < 2) {
        exit("Usage: ToBaseX <collection> <inputDir> [L:LIMIT]")
    }
    val collection = args[0]
    val inputDir = File(args[1]).apply { if (!isDirectory) exit("Invalid inputDir: $absolutePath") }
    var limit: Long = args.find { it.startsWith("L:") }?.substring(2)?.toLong() ?: Long.MAX_VALUE

    val context = Context()
    CreateDB(collection).execute(context)
    inputDir.listFiles(FileFilter { it.isFile })?.forEach files@ { file ->
        try {
            if (file.name.toLowerCase().endsWith(".xml")) {
                if (limit-- <= 0L) return@files
                Add(file.name, file.canonicalPath).execute(context)
            } else {
                val isaList = ISA.read(file)
                when {
                    isaList.isEmpty() -> warning("ISA not found in $file")
                    isaList.size == 1 -> context.add("${file.name}.xml", isaList.first())
                    else -> {
                        isaList.forEachIndexed { i, isa ->
                            if (limit-- <= 0L) return@files
                            context.add("${file.name}.part-${i + 1}(${isaList.size}).xml", isa)
                        }
                    }
                }
            }
        } catch (e: Exception) {
            println("Invalid ${file.name}")
            e.printStackTrace()
        }
    }
    Optimize().execute(context)

    println("* Database information:")
    println(InfoDB().execute(context))

    context.close()
}

fun warning(message: String) {
    println("WARNING: " + message)
}

private fun Context.add(path: String, isa: ISA) {
    print("$path: ")
    if (isa.valid) {
        println("${isa.stat.docType} CLP: ${isa.stat.clpCount} at ${isa.position}")
    } else {
        println("${isa.stat} at ${isa.position}")
        println(isa.code)
        return
    }

    val xml = isa.toXML()
    try {
        with(Add(path)) {
            setInput(xml.inputStream())
            execute(this@add)
        }
    } catch(e: Exception) {
        e.printStackTrace()
        println("ISA: " + isa.code)
        if (e !is EDISyntaxException) {
            println("XML: " + xml.toString(ISA.XML_CHARSET))
        }
    }
}

private fun exit(message: String) {
    println(message)
    exitProcess(1)
}