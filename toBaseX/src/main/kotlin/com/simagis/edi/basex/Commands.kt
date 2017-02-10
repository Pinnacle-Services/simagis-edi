package com.simagis.edi.basex

import com.berryworks.edireader.util.CommandLine
import java.io.File
import kotlin.system.exitProcess

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/9/2017.
 */
internal operator fun CommandLine.get(i: Int): String? = getPosition(i)
internal operator fun CommandLine.get(option: String): String? = getOption(option)
internal fun warning(message: String) {
    println("WARNING: " + message)
}

internal fun File.exitIfNotIsDir(name: String): File = apply {
    if (!isDirectory) exit("Invalid $name: $absolutePath")
}

internal fun exit(message: String) {
    println(message)
    exitProcess(1)
}