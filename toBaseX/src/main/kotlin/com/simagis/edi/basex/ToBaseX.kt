package com.simagis.edi.basex

import com.berryworks.edireader.EDISyntaxException
import com.berryworks.edireader.demo.EDItoXML
import org.basex.core.BaseXException
import org.basex.core.Context
import org.basex.core.cmd.Add
import org.basex.core.cmd.CreateDB
import org.basex.core.cmd.InfoDB
import org.basex.core.cmd.Optimize
import java.io.*
import kotlin.system.exitProcess


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/1/2017.
 */

fun main(args: Array<String>) {
    if (args.size < 2) {
        exit("Usage: ToBaseX <collection> <inputDir>")
    }
    val collection = args[0]
    val inputDir = File(args[1]).apply { if (!isDirectory) exit("Invalid inputDir: $absolutePath") }

    val context = Context()
    CreateDB(collection).execute(context)
    inputDir.listFiles(FileFilter { it.isFile })?.forEach {
        if (it.name.toLowerCase().endsWith(".xml")) {
            Add(it.name, it.canonicalPath).execute(context)
        } else {
            try {
                with(Add("${it.name}.xml")) {
                    setInput(it.toXml())
                    execute(context)
                }
            } catch(e: BaseXException) {
                e.printStackTrace()
                println(it.toXml().reader().readText())
            }
        }
    }
    Optimize().execute(context)

    println("\n* Show database information:")
    println(InfoDB().execute(context))

    context.close()
}

private fun File.toXml(): InputStream {
    val result = ByteArrayOutputStream()
    bufferedReader().use { reader ->
        OutputStreamWriter(result, "ISO-8859-1").use { writer ->
            EDItoXML(reader, writer).run()
        }
    }
    return ByteArrayInputStream(result.toByteArray())
}

private fun exit(message: String) {
    println(message)
    exitProcess(1)
}