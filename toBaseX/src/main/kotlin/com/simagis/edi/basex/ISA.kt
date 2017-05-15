package com.simagis.edi.basex

import com.berryworks.edireader.EDIReader
import com.berryworks.edireader.EDIReaderFactory
import com.berryworks.edireader.EDISyntaxException
import org.xml.sax.Attributes
import org.xml.sax.InputSource
import org.xml.sax.helpers.DefaultHandler
import java.io.*
import javax.xml.transform.OutputKeys
import javax.xml.transform.TransformerFactory
import javax.xml.transform.sax.SAXSource
import javax.xml.transform.stream.StreamResult

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/1/2017.
 */
class ISA private constructor(text: String, private val start: Int, private val end: Int) {
    val code: String = when (end) {
        -1 -> text.substring(start)
        else -> text.substring(start, end)
    }

    val position: String get() = "$start.." + if (end == -1) "end" else "$end"
    val valid: Boolean get() = stat.status == Status.VALID
    val stat: Stat by lazy { Stat(code) }
    val type: String get() = if (valid) stat.doc.type?: "none" else "invalid"
    val dbName: String get() = with(stat.doc) {
        when {
            date?.length == 8 -> "$type-${date?.take(6)}"
            else -> type ?: "unknown"
        }
    }

    fun toXML(): ByteArray {
        val result = ByteArrayOutputStream()
        OutputStreamWriter(result, CHARSET).use { writer ->
            val ediReader = EDIReader()
            val source = SAXSource(ediReader, code.inputSource)
            val transformer = TransformerFactory.newInstance().newTransformer()
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes")
            transformer.transform(source, StreamResult(writer))
        }
        return result.toByteArray()
    }

    fun toXmlCode(): String? = try {
        toXML().toString(ISA.CHARSET)
    } catch(e: Exception) {
        null
    }

    override fun toString(): String = code

    companion object {
        val CHARSET = Charsets.ISO_8859_1

        fun of(code: String): ISA = ISA(code, 0, -1)

        fun read(file: File): List<ISA> = mutableListOf<ISA>().apply {
            val (terminator, s1) = Delimiters.of(file)
            val text = file.reader(CHARSET).use {
                filter(it, terminator, StringBuilder(file.length().toInt()))
            }
            var index: Int = 0
            var next: Int
            val isaPattern = "${terminator}ISA$s1"
            while (true) {
                next = text.indexOf(isaPattern, index + isaPattern.length, false)
                this += ISA(text.toString(), index, next)
                if (next == -1) break
                index = next + 1
                if (index >= text.length) break
            }
        }

        internal fun filter(reader: Reader, terminator: Char, builder: StringBuilder) : StringBuilder {
            val buff = CharArray(4096)
            var tMode = false
            while (true) {
                val res = reader.read(buff)
                if (res == -1) break
                @Suppress("LoopToCallChain")
                for (i in 0..res - 1) {
                    val c = buff[i]
                    if (tMode) {
                        if (c > ' ') {
                            tMode = false
                            builder.append(c)
                        }
                    } else {
                        if (c >= ' ') builder.append(c)
                    }
                    if (c == terminator) tMode = true
                }
            }
            return builder
        }

        private val String.inputSource: InputSource get() = InputSource(StringReader(this))

        private data class Delimiters(
                val terminator: Char = '~',
                val separator1: Char = '*',
                val separator2: Char = ':') {
            companion object {
                fun of(file: File): Delimiters {
                    val header = file.reader(CHARSET).use {
                        val buff = CharArray(1024)
                        var offset = 0
                        var length = buff.size
                        while (length > 0) {
                            val res = it.read(buff, offset, length)
                            if (res == -1) break
                            length -= res
                            offset += res
                        }
                        StringBuilder(1024).apply {
                            @Suppress("LoopToCallChain")
                            for (c in buff) if (c >= ' ') append(c)
                        }.toString()
                    }
                    if (!header.startsWith("ISA"))
                        throw EDISyntaxException("Invalid file $file: starting 'ISA' not found")
                    if (header.length < 106)
                        throw EDISyntaxException("Invalid file $file: length < 106")
                    return Delimiters(
                            terminator = header[105],
                            separator1 = header[103],
                            separator2 = header[104])
                            .apply {
                                if (header[3] != separator1) {
                                    throw EDISyntaxException(
                                            "Invalid file $file: invalid separator1: '$separator1' or starting 'ISA$separator1' not found")
                                }
                            }
                }
            }
        }
    }

    data class Doc(var type: String? = null, var date: String? = null, var time: String? = null)

    data class Counts(var clp: Int = 0)

    enum class Status {
        VALID,
        INVALID,
        ERROR,
    }

    class Stat(code: String) {
        var status = Status.INVALID
        val doc = Doc()
        val counts = Counts()
        var error: Exception? = null

        init {
            try {
                val parser = EDIReaderFactory.createEDIReader(code.inputSource, null)
                parser.contentHandler = object : DefaultHandler() {
                    override fun startElement(uri: String?, localName: String?, qName: String?, attributes: Attributes?) {
                        when (qName) {
                            "group" -> {
                                doc.date = attributes["Date"]
                                doc.time = attributes["Time"]
                            }
                            "transaction" -> {
                                doc.type = attributes["DocType"]
                            }
                            "segment" -> {
                                if (attributes["Id"] == "CLP") {
                                    counts.clp++
                                }
                            }
                        }
                    }

                    private operator fun Attributes?.get(name: String): String? = this?.getValue(name)
                }
                parser.parse(code.inputSource)
                status = when (doc.type) {
                    "835", "837" -> Status.VALID
                    else -> Status.INVALID
                }
            } catch(e: EDISyntaxException) {
                status = Status.ERROR
                error = e
            }
        }

        override fun toString(): String {
            return "Stat(status=$status, doc=$doc, counts=$counts, error=$error)"
        }

    }
}

