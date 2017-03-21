package com.simagis.edi.mdb

import org.bson.Document
import org.junit.Assert.*
import org.junit.Test

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/21/2017.
 */
class DocBuilderTest {

    @Test
    fun init() {
        val doc: Document = doc {
            `+`("a", "b")
            `+`("b" to doc { `+`("n", 123) })
            `+`(`$`("set") to "1", `$`("abc") to "1")
        }

        assertEquals(
                doc.toJson(),
                """{ "a" : "b", "b" : { "n" : 123 }, """" + '$' + """set" : "1", """" + '$' + """abc" : "1" }"""
        )
    }

}
