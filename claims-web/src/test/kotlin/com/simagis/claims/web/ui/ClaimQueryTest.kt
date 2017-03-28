package com.simagis.claims.web.ui

import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/28/2017.
 */
class ClaimQueryTest

private val pp = JsonWriterSettings(JsonMode.SHELL, true)

fun main(args: Array<String>) {
    val test = ClaimQueryTest::class.java.let {
        it.getResourceAsStream("${it.simpleName}.json").use {
            Document.parse(it.reader().readText())
        }
    }

    val doc = test["in"] as Document
    println("doc: ${doc.toJson(pp)}")

    val parameters = test["parameters"] as Document
    val request: (String) -> String? = { key ->
        parameters[key] as String?
    }

    val doc2 = doc.applyParameters(request)
    println("doc2: ${doc2.toJson(pp)}")
}