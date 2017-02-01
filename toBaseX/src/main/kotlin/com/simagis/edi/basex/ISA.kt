package com.simagis.edi.basex

import com.berryworks.edireader.EDIReader
import com.berryworks.edireader.EDIReaderFactory
import com.berryworks.edireader.EDISyntaxException
import org.xml.sax.Attributes
import org.xml.sax.InputSource
import org.xml.sax.helpers.DefaultHandler
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.OutputStreamWriter
import java.io.StringReader
import javax.xml.transform.TransformerFactory
import javax.xml.transform.sax.SAXSource
import javax.xml.transform.stream.StreamResult

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/1/2017.
 */
class ISA private constructor(private val text: String, private val start: Int, private val end: Int) {
    val code: String
        get() = when (end) {
            -1 -> text.substring(start)
            else -> text.substring(start, end)
        }

    val position: String get() = "$start.." + if (end == -1) "end" else "$end"
    val valid: Boolean get() = stat.status == Status.VALID
    val stat: Stat by lazy { Stat(code) }

    fun toXML(): ByteArray {
        val result = ByteArrayOutputStream()
        OutputStreamWriter(result, XML_CHARSET).use { writer ->
            val ediReader = EDIReader()
            val source = SAXSource(ediReader, InputSource(StringReader(code)))
            val transformer = TransformerFactory.newInstance().newTransformer()
            transformer.transform(source, StreamResult(writer))
        }
        return result.toByteArray()
    }

    override fun toString(): String = code

    companion object {
        val XML_CHARSET = Charsets.ISO_8859_1

        fun read(file: File): List<ISA> = mutableListOf<ISA>().apply {
            val text = file.readText().replace("\n", "").replace("\r", "")
            val separator = findSeparator(text)
            var index = 0
            var next = 0
            val isaPattern = "ISA$separator"
            while (index != -1 && next != -1) {
                next = text.indexOf(isaPattern, index + 4, false)
                this += ISA(text, index, next)
                index = text.indexOf(isaPattern, next, false)
            }
        }

        private fun findSeparator(text: String): Char {
            if (text.length < 108) throw EDISyntaxException("text.length < 108")
            if (!text.startsWith("ISA")) throw EDISyntaxException("starting 'ISA' not found")
            val tildeIndex = text.indexOf("~")
            if (tildeIndex != 105) throw EDISyntaxException("starting '~' not found")
            val a = text[tildeIndex - 2]
            val gsIndex = text.indexOf("GS$a", tildeIndex)
            when {
                gsIndex == -1 -> throw EDISyntaxException("starting '~GS$a' not found")
                gsIndex == tildeIndex + 1 -> {
                }
                text.substring(tildeIndex + 1, gsIndex).isNotBlank() -> throw EDISyntaxException(
                        "starting '~GS$a' contains not blank separator: ${text.substring(tildeIndex, gsIndex + 2)}")
            }
            return a
        }
    }

    enum class Status {
        VALID,
        INVALID,
        ERROR,
    }

    class Stat(code: String) {
        var status = Status.INVALID
        var docType: String? = null
        var clpCount: Int = 0
        var error: Exception? = null

        init {
            try {
                val leftOver = null
                val parser = EDIReaderFactory.createEDIReader(InputSource(StringReader(code)), leftOver)
                parser.contentHandler = object : DefaultHandler() {
                    override fun startElement(uri: String?, localName: String?, qName: String?, attributes: Attributes?) {
                        when(qName) {
                            "transaction" ->
                                if (docType == null) {
                                    docType = attributes?.getValue("DocType")
                                }
                            "segment" ->
                                if ("CLP" == attributes?.getValue("Id")) {
                                    clpCount++
                                }
                        }
                    }
                }
                parser.parse(InputSource(StringReader(code)))
                status = when (docType) {
                    "835", "837" -> Status.VALID
                    else -> Status.INVALID
                }
            } catch(e: EDISyntaxException) {
                status = Status.ERROR
                error = e
            }
        }

        override fun toString(): String {
            return "Stat(status=$status, docType=$docType, clpCount=$clpCount, error=$error)"
        }
    }
}

