package com.simagis.edi.mongodb

import java.time.Instant
import java.util.*


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 12/15/2017.
 */
fun main(args: Array<String>) {
    val instant = Instant.parse("2017-01-01T12:00:00.000Z")
    println(instant)
    println(Date.from(instant))
}
