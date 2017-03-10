package com.simagis.edi.basex

import org.basex.core.Context
import org.basex.core.MainOptions
import org.basex.core.cmd.CreateDB
import org.basex.core.cmd.DropDB
import org.basex.core.cmd.Replace
import org.basex.core.cmd.XQuery
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import javax.json.Json
import javax.json.JsonObject
import kotlin.concurrent.withLock

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/9/2017.
 */
fun main(args: Array<String>) {
    val lock: Lock = ReentrantLock()
    val contexts: ThreadLocal<Context> = ThreadLocal.withInitial {
        Context().apply {
            options.set(MainOptions.MAINMEM, true)
            CreateDB("temp").execute(this)
        }
    }
    List(100_000) {
        """
        <top>
        <a><b>$it</b></a>
        <a><b>${it + 1}</b></a>
        <a><b>${it * 2}</b></a>
        <a><b>abc${it % 7}</b></a>
        </top>"""
    }.stream().parallel().forEach {
        val context = contexts.get()

        with(Replace("doc")) {
            setInput(it.byteInputStream())
            execute(context)
        }

        with(XQuery("""
        declare option output:method "json";
        <json type='array'>{
            for ${"$"}a in collection()//a
            return
                <_ type='object'>
                    <a>{${"$"}a/b/text()}</a>
                </_>
        }
        </json>
        """)) {
            val json = execute(context)
            val jsonArray = Json.createReader(json.reader()).readArray()
            val jsonObject = jsonArray[0] as JsonObject
            if (jsonObject.getString("a").toInt() % 100 == 0) {
                lock.withLock {
                    println(jsonArray.toString())
                }
            }
        }
    }
}
