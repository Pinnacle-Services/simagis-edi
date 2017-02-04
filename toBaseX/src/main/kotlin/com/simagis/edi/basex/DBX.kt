package com.simagis.edi.basex

import org.basex.core.Context
import org.basex.core.cmd.CreateDB
import java.io.Closeable

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/2/2017.
 */
class DBX : Closeable {
    private val contextMap = mutableMapOf<String, Context>()
    val names: Set<String> get() = contextMap.keys

    fun on(name: String, action: (Context) -> Unit) = action(
            contextMap.getOrPut(name) { Context().apply { CreateDB(name).execute(this) } }
    )

    override fun close() {
        contextMap.values.forEach {
            try {
                it.close()
            } catch(e: Exception) {
                e.printStackTrace()
            }
        }
        contextMap.clear()
    }
}
