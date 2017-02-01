package com.simagis.edi.basex

import com.berryworks.edireader.EDIReader
import com.berryworks.edireader.EDISyntaxException
import org.xml.sax.InputSource
import java.io.*
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

    fun toXML(): ByteArray {
        val result = ByteArrayOutputStream()
        OutputStreamWriter(result, XML_CHARSET).use { writer ->
            val reader = StringReader(code)
            val ediReader = EDIReader()
            val source = SAXSource(ediReader, InputSource(reader))
            val transformer = TransformerFactory.newInstance().newTransformer()
            transformer.transform(source, StreamResult(writer))
        }
        return result.toByteArray()
    }

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
                gsIndex == tildeIndex + 1 -> {}
                text.substring(tildeIndex + 1, gsIndex).isNotBlank() -> throw EDISyntaxException(
                        "starting '~GS$a' contains not blank separator: ${text.substring(tildeIndex, gsIndex + 2)}")
            }
            return a
        }
    }
}

