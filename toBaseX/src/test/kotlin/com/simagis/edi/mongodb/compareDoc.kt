package com.simagis.edi.mongodb

import org.bson.Document

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/10/2017.
 */
fun main(args: Array<String>) {
    val d1 = Document("a", "a")
            .append("c", "c")
            .append("d", "d")

    val d2 = Document("a", "a")
            .append("d", "d")
            .append("c", "c")

    val d3 = Document(d2)

    println("$d1 == $d2 > ${d1 == d2}")
    println("$d1 == $d3 > ${d1 == d3}")
}