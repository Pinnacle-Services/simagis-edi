package com.simagis.claims.web

import com.mongodb.client.MongoDatabase
import com.simagis.edi.mdb.*
import org.bson.Document
import java.lang.Long.max
import java.lang.Long.min
import java.text.SimpleDateFormat
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonString

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/14/2017.
 */
class Claims835ToHtml(val db: MongoDatabase,
                      val maxCount: Int = 100,
                      val paging: Paging = Paging(0, 0),
                      val root: String,
                      val queryString: String) {
    private var count = 0
    private var indent = 0
    private val html = StringBuilder()

    private fun formatKey(key: String, value: Any?): String = keys.map.getOrDefault(key, key)

    private fun formatValue(key: String, value: Any?): String = when {
        key == "_id" && value is String -> value
        key == "cpt" && value is String -> value + " " + cptCodes.optString(value).esc
        key == "status" && value is String -> value + " " + statusCodes.optString(value).esc
        key == "adjGrp" && value is String -> value + " " + adjGrpCodes.optString(value).esc
        key == "adjReason" && value is String -> value + " " + adjReasonCodes.optString(value).esc
        key == "rem" && value is String -> value + " " + remCodes.optString(value).esc
        key == "dxT" && value is String -> value + " " + dxT.optString(value).esc
        key == "dxV" && value is String -> value + " " + icd10Codes.optString(value).esc
        value is Date -> dateFormat.format(value).esc
        else -> value.asHTML.esc
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
                order += key
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
        private val icd10Codes: Map<String, String> by lazy {
            loadAsMap("icd10-codes.json", {
                it.getString("icd10_code_id") to it.getString("description")
            })
        }
        private val dxT: Map<String, String> by lazy {
            loadAsMap("dxT-codes.json", {
                it.getString("id") to it.getString("caption")
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

    fun append(c835: Document): Boolean {
        val claim = Document(c835)

        val c835procDate = c835["procDate"] as? Date
        if (c835procDate != null) {
            val c837 = db["claims_837"]
                    .find(doc {
                        `+`("acn", c835["acn"])
                        `+$lt`("sendDate", c835procDate)
                    })
                    .projection(doc {
                        `+`("dx", 1)
                        `+`("npi", 1)
                        `+`("drFirsN", 1)
                        `+`("drLastN", 1)
                        `+`("sendDate", 1)
                    })
                    .sort(doc {
                        `+`("sendDate", -1)
                    })
                    .first()
            if (c837 != null) {
                claim["c837"] = Document(c837)
            }
        }

        addClaimHeader(claim)
        addClaimBody(claim)
        addClaimFooter(claim)
        return count++ <= maxCount
    }

    private fun addClaimHeader(claim: Document) {
        html.append("<a name='${claim._id}'></a><br><br>")
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
        if (key.isBlank()) {
            addIndentedText("{")
        } else {
            addIndentedText(keyToHTML(formatKey(key, 0)) + ":{")
        }
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
        addIndentedText(keyToHTML(formattedKey) + ": " + separator + formatValue(key, value))
    }

    private fun addClaimFooter(claim: Document) {
        addIndentedText("<hr>")
    }

    private fun addPageHeader() {
        html.append("""<body style='font-family:monospace'>
            <br>
            <br>
            <div style="position: fixed; overflow: auto; top: 0; left: 0; right: 0; background-color: #fff; padding: 5px">
        """)
        addPageNavigator()
        html.append("<hr></div>\n")
    }

    private fun addPageFooter() {
        addPageNavigator()
        html.append("</body>")
    }

    private fun addPageNavigator() {
        fun href(ps: Long, pn: Long): String {
            val query = queryString
                    .split("&")
                    .filter { !(it.startsWith("pn=") || it.startsWith("ps=")) }
                    .joinToString(separator = "&")
            val paging = "ps=$ps&pn=$pn"
            val separator = if (query.isEmpty()) "" else "&"
            return "$root?$query$separator$paging"
        }
        if (paging.isPageable) {
            if (paging.pn > 0) addLink(href(paging.ps, paging.pn - 1), "<- Previous ${paging.ps}".esc)
            html.append(" Records ${paging.pn * paging.ps + 1}-${min(paging.found, (paging.pn + 1) * paging.ps)} of ${paging.found} ")
            if (paging.pn + 1 < paging.pageCount) addLink(href(paging.ps, paging.pn + 1), "Next ${paging.ps} ->".esc )
        } else {
            if (count != 1) {
                if (count == maxCount) {
                    addIndentedText("found $count claims (or more)")
                } else {
                    addIndentedText("found $count claims")
                }
            }
        }
    }


    private fun keyToHTML(key: String) = "<span style='color:gray'>${key.esc}</span>"

    private fun addIndentedText(html: String) {
        this.html.append("<div style='text-indent: ${indent}em;'>$html</div>\n")
    }

    private fun addLink(href: String = "", html: String, separator: String = "\n", fragment: String = "") {
        this.html.append("<a href='$href#$fragment'>$html</a>$separator")
    }

    private val String.esc get() = this.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    private val Any?.asHTML: String get() = this?.toString() ?: ""

}

