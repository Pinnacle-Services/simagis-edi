package com.simagis.claims.web

import org.bson.Document
import java.text.SimpleDateFormat
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonString

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/14/2017.
 */
class Claims835ToHtml(val maxCount: Int = 100, val paging: Paging = Paging(0, 0), val root: String) {
    private var count = 0
    private var indent = 0
    private val html = StringBuilder()

    private fun formatKey(key: String, value: Any?): String = keys.map.getOrDefault(key, key)

    private fun formatValue(key: String, value: Any?): String = when {
        key == "cpt" && value is String -> value + " " + cptCodes.optString(value)
        key == "status" && value is String -> value + " " + statusCodes.optString(value)
        key == "adjGrp" && value is String -> value + " " + adjGrpCodes.optString(value)
        key == "adjReason" && value is String -> value + " " + adjReasonCodes.optString(value)
        key == "rem" && value is String -> value + " " + remCodes.optString(value)
        value is Date -> dateFormat.format(value)
        else -> value.asHTML
    }

    private class Keys(private val order: List<String>, val map: Map<String, String>) {

        fun orderedKeys(claim: Document): List<String> = mutableListOf<String>().apply {
            val inst = claim.keys.sorted()
            this += order.filter { inst.contains(it) }
            this += inst.filter { !order.contains(it) }
        }

    }

    companion object {
        private val dateFormat = SimpleDateFormat("yyyy-MM-dd")

        private val keys: Keys by lazy {
            val order = mutableListOf<String>()
            val map = loadAsMap("claim835.keyNames.json", {
                val key = it.getString("key")
                order+= key
                key to it.getString("name")
            })
            Keys(order, map)
        }

        private val cptCodes: Map<String, String> by lazy {
            loadAsMap("claim835-cpt-codes.json", {
                it.getString("cpt_code") to it.getString("short_description")
            })
        }
        private val statusCodes: Map<String, String> by lazy {
            loadAsMap("claim835-status-codes.json", {
                it.getString("id") to it.getString("caption")
            })
        }
        private val adjGrpCodes: Map<String, String> by lazy {
            loadAsMap("claim835-adjGrp-codes.json", {
                it.getString("id") to it.getString("caption")
            })
        }
        private val adjReasonCodes: Map<String, String> by lazy {
            loadAsMap("claim835-adjReason-codes.json", { json ->
                val reason = json.getString("Reason")
                val description = json["Description"]
                if (reason is String && description is JsonString)
                    reason to description.string else null
            })
        }
        private val remCodes: Map<String, String> by lazy {
            loadAsMap("claim835-rem-codes.json", {
                it.getString("Remark") to it.getString("Description")
            })
        }

        private fun loadAsMap(name: String, map: (JsonObject) -> Pair<String, String>?): Map<String, String> = mutableMapOf<String, String>().apply {
            Claims835ToHtml::class.java.getResourceAsStream(name).use { Json.createReader(it).readArray() }.forEach {
                if (it is JsonObject) {
                    map(it)?.let { this += it }
                }
            }
        }

        private fun Map<String, String>.optString(key: String, def: String = ""): String {
            return this[key] ?: def
        }
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
        for (key in keys.orderedKeys(doc)) {
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
        val formattedKey = formatKey(key, value)
        val separator = "&nbsp;".repeat(maxKeyLength - formattedKey.length)
        addIndentedText(keyToHTML(formattedKey) + ": " + separator + formatValue(key, value).esc)
    }

    private fun addClaimFooter(claim: Document) {
        addIndentedText("<hr>")
    }

    private fun addPageHeader() {
        html.append("<body style='font-family:monospace'>")
    }

    private fun addPageFooter() {
        if (paging.isPageable) {
            if (paging.pn > 0) addLink("$root?ps=${paging.ps}&pn=${paging.pn - 1}", "<-- Back".esc)
            if (count == paging.ps) addLink("$root?ps=${paging.ps}&pn=${paging.pn + 1}", "Next -->".esc)
        } else {
            if (count != 1) {
                if (count == maxCount) {
                    addIndentedText("found $count claims (or more)")
                } else {
                    addIndentedText("found $count claims")
                }
            }
        }

        html.append("</body>")
    }


    private fun keyToHTML(key: String) = "<span style='color:gray'>${key.esc}</span>"

    private fun addIndentedText(html: String) {
        this.html.append("<div style='text-indent: ${indent}em;'>$html</div>\n")
    }

    private fun addLink(href: String, html: String) {
        this.html.append("<a href='$href'>$html</a>\n")
    }

    private val String.esc get() = this.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    private val Any?.asHTML: String get() = this?.toString() ?: ""

}

