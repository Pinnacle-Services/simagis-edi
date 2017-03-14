package com.simagis.claims.web

import org.bson.Document
import java.text.SimpleDateFormat
import java.util.*

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/14/2017.
 */
class ClaimsToHtml(val maxCount: Int = 100) {
    private var count = 0
    private var indent = 0
    private val html = StringBuilder()
    private val dateFormat = SimpleDateFormat("yyyy-MM-dd")

    private fun formatKey(key: String, value: Any?): String = when {
        else -> key
    }

    private fun formatValue(key: String, value: Any?): String = when {
        value is Date -> dateFormat.format(value)
        else -> value.asHTML
    }

    init {
        addPageHeader()
    }

    fun toBytes(): ByteArray {
        addPageFooter()
        return html.toString().toByteArray()
    }

    fun append(claim: Document): Boolean {
        addClaimHeader(claim)
        addClaimBody(claim)
        addClaimFooter(claim)
        return count++ <= maxCount
    }

    private fun addClaimHeader(claim: Document) {

    }

    private fun addClaimBody(claim: Document) {
        addDoc(claim)
    }

    private fun addDoc(doc: Document) {
        val maxKeyLength: Int = doc.keys.map { formatKey(it, null).length }.max() ?: 0
        for (key in orderedKeys(doc)) {
            val value = doc[key]
            when (value) {
                is Document -> addDocBody(key, value)
                is List<*> -> {
                    addArrayHeader(key, value.size)
                    addArrayBody(value)
                    addArrayFooter(key, value.size)
                }
                else -> addProperty(maxKeyLength, key, value)
            }
        }
    }

    private fun addArrayHeader(key: String, size: Int) {
        addIndentedText(keyToHTML(formatKey(key, size)) + ":[")
    }

    private fun addArrayBody(value: List<*>) {
        indent++
        value.filterIsInstance<Document>().forEach { addDocBody("", it) }
        indent--
    }

    private fun addArrayFooter(key: String, size: Int) {
        addIndentedText("]")
    }

    private fun addDocHeader(key: String, value: Document) {
        addIndentedText("{")
    }

    private fun addDocBody(key: String, value: Document) {
        addDocHeader(key, value)
        indent++
        addDoc(value)
        indent--
        addDocFooter(key, value)
    }

    private fun addDocFooter(key: String, value: Document) {
        addIndentedText("}")
    }

    private fun addProperty(maxKeyLength: Int, key: String, value: Any?) {
        addIndentedText("${keyToHTML(formatKey(key, value))}: ${"&nbsp;".repeat(maxKeyLength - key.length)}${formatValue(key, value).esc}")
    }

    private fun addClaimFooter(claim: Document) {
        addIndentedText("<hr>")
    }

    private fun addPageHeader() {
        html.append("<body style='font-family:monospace'>")
    }

    private fun addPageFooter() {
        if (count != 1) {
            if (count == maxCount) {
                addIndentedText("found $count claims (or more)")
            } else {
                addIndentedText("found $count claims")
            }
        }
        html.append("</body>")
    }

    private fun orderedKeys(claim: Document) = claim.keys.sorted()

    private fun keyToHTML(key: String) = "<span style='color:gray'>${key.esc}</span>"

    private fun addIndentedText(html: String) {
        this.html.append("<div style='text-indent: ${indent}em;'>$html</div>\n")
    }

    private val String.esc get() = this.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    private val Any?.asHTML: String get() = this?.toString() ?: ""

}

