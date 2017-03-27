package com.simagis.claims.web.ui

import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
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
    val doc = Document.parse("""
{
  "a" : "#",
  "b" : "#=",
  "c" : "#=1",
  "st1" : "#=st1",
  "cur1" : "#cur",
  "cur2" : "#cur=3.45",
  "num1" : "#int=-1.23",
  "num2" : "#int",
  "int1" : "#int=-1",
  "int2" : "#int",
  "date1" : "#date=2016-03-27",
  "date2" : "#date",
  "arr" : [{
      "arrDate1" : "#date=2016-03-27",
      "arrDate2" : "#date"
      "arrStr1" : {"${'$'}gt": "`aStr1`"}
      "arrStr2" : {"${'$'}gt": "`aStr2=abc`"}
      "arrInt1" : {"${'$'}gt": "`aInt1:int`"}
      "arrInt2" : {"${'$'}gt": "`aInt2:int=42`"}
    }]
}
""")

    println("doc: ${doc.toJson(pp)}")

    val request: (String) -> String? = { key ->
        when (key) {
            "st1" -> "st1+1"
            "cur1" -> "1.23"
            "num2" -> "12.3"
            "int2" -> "123"
            "date2" -> "2017-03-27"
            "arrDate2" -> "2017-01-31"
            else -> null
        }
    }

    val doc2 = doc.applyParameters(request)
    println("doc2: ${doc2.toJson(pp)}")
}