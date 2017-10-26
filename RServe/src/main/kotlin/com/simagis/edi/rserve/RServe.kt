package com.simagis.edi.rserve

import org.rosuda.REngine.REXP
import org.rosuda.REngine.Rserve.RConnection

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 10/25/2017.
 */

class RServe(val host: String = "127.0.0.1",
             val port: Int = 6311,
             val debugR: Boolean = false,
             val debugRserve: Boolean = false
) {
    private fun connect(): RConnection = try {
        RConnection(host, port)
    } catch (e: Throwable) {
        R.launch(
                "-e",
                "library(Rserve); Rserve(${debugRserve.toString().toUpperCase()}, args='--no-save --slave')",
                "--no-save",
                "--slave",
                debug = debugR) {
            waitFor()
            RConnection(host, port)
        }
    }

    private inline fun <reified T> call(action: RConnection.() -> T): T {
        val rConnection = connect()
        try {
            return rConnection.action()
        } finally {
            rConnection.close()
        }
    }

    infix fun eval(cmd: String): REXP = call<REXP> { eval(cmd) }

    infix fun exec(cmd: String): Unit = call { voidEval(cmd) }

    fun shutdown(): Unit = call { shutdown() }
}
