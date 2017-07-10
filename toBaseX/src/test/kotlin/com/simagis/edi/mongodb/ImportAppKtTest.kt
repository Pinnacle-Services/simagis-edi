package com.simagis.edi.mongodb

import org.bson.Document
import org.intellij.lang.annotations.Language
import org.junit.Test
import kotlin.test.assertEquals

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 7/11/2017.
 */
class ImportAppKtTest {
    @Test
    fun augment835PR() {
        @Language("JSON")
        val document = Document.parse("""{ "svc": [
          { "adj": [
            { "adjGrp": "PR", "adjAmt": 1.2 },
            { "adjGrp": "PR", "adjAmt": 3.4 } ],
            "cptPay": 123.0
          }] }""")
        @Language("JSON")
        val document2 = Document.parse("""{ "svc": [
          { "adj": [
            { "adjGrp": "PR", "adjAmt": 1.2 },
            { "adjGrp": "PR", "adjAmt": 3.4 } ],
            "cptPay": 123.0,
            "cptPr" : 4.6,
            "cptAll" : 127.6
          }] }""")
        document.augment835()
        assertEquals(document, document2)
    }

    @Test
    fun augment835CO() {
        @Language("JSON")
        val document = Document.parse("""{ "svc": [
          { "adj": [
            { "adjGrp": "CO", "adjAmt": 1.2 },
            { "adjGrp": "CO", "adjAmt": 3.4 } ],
            "cptPay": 123.0
          }] }""")
        @Language("JSON")
        val document2 = Document.parse("""{ "svc": [
          { "adj": [
            { "adjGrp": "CO", "adjAmt": 1.2 },
            { "adjGrp": "CO", "adjAmt": 3.4 } ],
            "cptPay": 123.0,
            "cptPr" : 0.0,
            "cptAll" : 123.0
          }] }""")
        document.augment835()
        assertEquals(document, document2)
    }

    @Test
    fun augment835None() {
        @Language("JSON")
        val document = Document.parse("""{ "svc": [
          { "cptPay": 123.0
          }] }""")
        @Language("JSON")
        val document2 = Document.parse("""{ "svc": [
          { "cptPay": 123.0,
            "cptPr" : 0.0,
            "cptAll" : 123.0
          }] }""")
        document.augment835()
        assertEquals(document, document2)
    }

}