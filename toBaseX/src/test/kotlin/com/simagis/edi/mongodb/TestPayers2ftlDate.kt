package com.simagis.edi.mongodb

import java.util.*

/**
 * Created by alexei.vylegzhanin@gmail.com on 7/25/2018.
 */
fun main(args: Array<String>) {
    val now = Date()
    println(now)
    val ftlDate = ImportJob.payers2ftlDate[now, "Medicare - Texas"]
    println(ftlDate)
}