package com.simagis.claims.web

import com.mongodb.client.MongoDatabase
import com.simagis.claims.web.ui.ClaimQuery
import com.simagis.edi.mdb.*
import org.bson.Document
import org.intellij.lang.annotations.Language
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
                      val cq: ClaimQuery?,
                      val maxCount: Int = 100,
                      val paging: Paging = Paging(0, 0),
                      val root: String,
                      val queryString: String) {
    private var count = 0
    private var indent = 0
    private val html = StringBuilder()

    private fun formatKey(key: String, value: Any? = null): String = keys.map.getOrDefault(key, key)

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

    fun append(doc: Document): Boolean {
        val claim = when (cq?.type) {
            "835" -> Document(doc).also { claim835 ->
                val c835procDate = doc["procDate"] as? Date
                if (c835procDate != null) {
                    val c837 = db["claims_837"]
                            .find(doc {
                                `+`("acn", doc["acn"])
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
                        claim835["c837"] = Document(c837)
                    }
                }
            }
            else -> doc
        }


        addClaimHeader(claim)
        html.claimFrame {
            addClaimBody(claim)
        }
        addClaimFooter(claim)
        return count++ <= maxCount
    }

    private fun addClaimHeader(claim: Document) {
        //language=HTML
        html.append(
            """
            <div style="padding: 5pt; background-color: #d4d4d4">
                <a name="${claim._id}"></a>
                <strong>${claim._id} | ${claim["prn"].esc}</strong>
                    (${claim.strongAmount("clmAsk")}
                    | ${claim.strongAmount("clmPayTotal")}
                    | ${claim.strongAmount("clmPay")}
                    | ${claim.strongAmount("pr")})

            </div>
            """
        )
    }

    private fun addClaimBody(claim: Document) {
        addDoc(claim)
    }

    private fun addDoc(doc: Document, context: ParentContext = ParentContext.DOC) {
        val maxKeyLength: Int = doc.keys.map { formatKey(it, null).length }.max() ?: 0
        for (key in keys.orderedKeys(doc)) {
            val value = doc[key]
            when (value) {
                is Document -> addDocBody(key, value)
                is List<*> -> addArray(key, value)
                else -> addProperty(maxKeyLength, key, value)
            }
        }
    }

    private fun addArray(key: String, value: List<*>, contex: ParentContext = ParentContext.DOC) {
        if (value.isNotEmpty()) {
            when (key) {
                "adj" -> addCptAdjustments(key, value, contex)
                else -> {
                    addArrayHeader(key, value.size)
                    addArrayBody(key, value)
                    addArrayFooter(key, value.size)
                }
            }
        }
    }

    private fun addCptAdjustments(key: String, value: List<*>, contex: ParentContext) {
        addIndented(keyToHTML(formatKey(key, value.size)) + ":")
        indent++
        //language=HTML
        addIndented(
            """
            <ul style="margin: 0">${value
                .mapNotNull {
                    when (it) {
                        is Document -> """
                            <li>
                                ${it.strongValue("adjGrp")} |
                                ${it.strongValue("adjReason", caption = "Reason: ".graySpan())} |
                                ${it.strongAmount("adjAmt", caption = "Amount: ".graySpan())}
                            </li>
                            """
                        else -> null
                    }
                }
                .joinToString(separator = "")}
            </ul>
            """
        )
        indent--
    }

    private fun addArrayHeader(key: String, size: Int) {
        addIndented(keyToHTML(formatKey(key, size)) + ":")
    }

    private fun addArrayBody(key: String, value: List<*>) {
        indent++
        html.arrayFrame {
            when (key) {
                "eob" -> addArrayBody837eob(key, value)
                else -> addArrayBodyAny(key, value)
            }
        }
        indent--
    }

    private data class ParentContext(
        val isArrayItem: Boolean = false,
        val isFirstArrayItem: Boolean = false,
        val isLastArrayItem: Boolean = false
    ) {
        companion object {
            val DOC = ParentContext()
        }
    }

    private fun addArrayBodyAny(key: String, value: List<*>) {
        value.forEachIndexed { index, item ->
            val context = ParentContext(
                true,
                index == 0,
                value.size == index + 1
            )
            when (item) {
                is Document -> addArrayDocBody(key, item, context)
                is List<*> -> addArray(key, item, context)
                else -> addArrayScalar(key, item, context)
            }
        }
    }

    private fun addArrayBody837eob(key: String, value: List<*>) {
        if (value.all { it is Document }) {
            addIndented(StringBuilder().apply {
                append("<table>")
                value.forEach {
                    val doc = it as Document
                    val id835 = doc["id835"] as? String
                    val procDate = doc["procDate"] as? Date
                    if (id835 != null && procDate != null) {
                        append("<tr><td>")
                        append(formatValue("procDate", procDate))
                        append("</td><td>")
                        append("<a href=/claim/835/$id835 target=_blank>$id835</a>")
                        append("</td></tr>\n")
                    }
                }
                append("</table>")
            }.toString())
        } else {
            addArrayBodyAny(key, value)
        }
    }

    private fun addArrayDocBody(key: String, value: Document, context: ParentContext) {
        addDocBody("", value, context)
    }

    private fun addArrayScalar(key: String, value: Any?, contex: ParentContext = ParentContext.DOC) {
        val str = when (key) {
            "eob" -> value.toString().let {
                "<a href=/claim/835/$it target=_blank>$it</a>"
            }
            else -> value.toString().esc
        }
        addIndented(str)
    }

    private fun addArrayFooter(key: String, size: Int) {
    }

    private fun addDocHeader(key: String, value: Document, context: ParentContext = ParentContext.DOC) {
        if (key.isBlank()) {
        } else {
            addIndented(keyToHTML(formatKey(key, 0)) + ":")
        }
    }

    private fun addDocBody(key: String, value: Document, context: ParentContext = ParentContext.DOC) {
        addDocHeader(key, value, context)
        indent++
        addDoc(value, context)
        indent--
        addDocFooter(key, value, context)
    }

    private fun addDocFooter(key: String, value: Document, context: ParentContext = ParentContext.DOC) {
        if (context.isArrayItem && !context.isLastArrayItem)
            addIndented("<hr>")
    }

    private fun addProperty(maxKeyLength: Int, key: String, value: Any?) {
        val formattedKey = formatKey(key, value)
        val separator = "&nbsp;".repeat(maxKeyLength - formattedKey.length)
        addIndented(keyToHTML(formattedKey) + ": " + separator + formatValue(key, value))
    }

    private fun addClaimFooter(claim: Document) {
        addIndented("<br>")
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
            if (paging.pn + 1 < paging.pageCount) addLink(href(paging.ps, paging.pn + 1), "Next ${paging.ps} ->".esc)
        } else {
            if (count != 1 && cq != null) {
                if (count == maxCount) {
                    addIndented("found $count claims (or more)")
                } else {
                    addIndented("found $count claims")
                }
            }
        }
    }


    private fun keyToHTML(key: String) = "<span style='color:gray'>${key.esc}</span>"

    private fun addIndented(html: String) {
        //language=HTML
        this.html.append("<div style='padding-left: ${indent}em;'>$html</div>\n")
    }

    private fun addLink(href: String = "", html: String, separator: String = "\n", fragment: String = "") {
        this.html.append("<a href='$href#$fragment'>$html</a>$separator")
    }

    private val String.esc get() = this.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    private val Any?.esc get() = when(this) {
        is String -> esc
        null -> ""
        else -> toString().esc
    }
    private val Any?.asHTML: String get() = this?.toString() ?: ""

    @Language("HTML")
    private fun String.graySpan(): String =
        """<span style="color:gray">$esc</span>"""

    @Language("HTML")
    private fun Document.strongAmount(key: String, caption: String = "") =
        """$caption<strong title="${formatKey(key).esc}">${'$'}${this[key].esc}</strong>"""


    @Language("HTML")
    private fun Document.strongValue(key: String, caption: String = "", default: String = "___"): String {
        val html = this[key]?.let { formatValue(key, it) } ?: default
        return """$caption<strong title="${formatKey(key).esc}">$html</strong>"""
    }

    private fun StringBuilder.claimFrame(inner: () -> Unit) {
        //language=HTML
        append("""<div style="padding-left: 32pt">""")
        inner()
        //language=HTML
        append( """</div>""" )
    }

    private fun StringBuilder.arrayFrame(inner: () -> Unit) {
        //language=HTML
        append("""<div style="border: solid darkgrey 1pt; padding: 8pt">""")
        inner()
        //language=HTML
        append( """</div>""" )
    }
}

