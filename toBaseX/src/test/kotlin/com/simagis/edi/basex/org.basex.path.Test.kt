package com.simagis.edi.basex

import org.basex.core.cmd.Replace
import java.io.File

/**
 *
 * Created by alexei.vylegzhanin@gmail.com on 2/23/2017.
 */
fun main(args: Array<String>) {
    System.setProperty("org.basex.path", File("/simagis/BaseX-test").absolutePath)
    DBX().on("TEST") { context ->
        with(Replace("123.xml")) {
            setInput("<test></test>".byteInputStream())
            execute(context)
        }
    }
}