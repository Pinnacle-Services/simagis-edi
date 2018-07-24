package com.simagis.claims.web

import com.mongodb.client.MongoDatabase
import com.simagis.claims.web.ui.ClaimQuery
import com.simagis.edi.mdb.*
import org.bson.Document
import org.intellij.lang.annotations.Language
import java.lang.Long.min
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.*

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/14/2017.
 */
class Claims835ToHtml(
    val db: MongoDatabase,
    val cq: ClaimQuery?,
    val maxCount: Int = 100,
    val paging: Paging = Paging(0, 0),
    val contextPath: String,
    val servletPath: String,
    val pathInfo: String,
    val root: String,
    val queryString: String
) {
    private var count = 0
    private var indent = 0
    private val html = StringBuilder()

    private fun formatKey(key: String, value: Any? = null): String = keys.map.getOrDefault(key, key)

    private fun formatValue(key: String, value: Any?): String = when {
        key == "_id" && value is String -> value
        key == "acn" && value is String -> ACN835.format(cq, contextPath, servletPath, pathInfo, value)
        key == "cpt" && value is String -> value + " " + cptCodes.optString(value).esc
        key == "status" && value is String -> value + " " + statusCodes.optString(value).esc
        key == "adjGrp" && value is String -> value + " " + adjGrpCodes.optString(value).esc
        key == "adjReason" && value is String -> value + " " + adjReasonCodes.optString(value).esc
        key == "rem" && value is String -> value + " " + remCodes.optString(value).esc
        key == "dxT" && value is String -> value + " " + dxT.optString(value).esc
        key == "dxV" && value is String -> value + " " + icd10Codes.optString(value).esc
        key == "npi" && value is String -> NPI.format(value)
        value is Date -> dateFormat.format(value).esc
        else -> value.asHTML.esc
    }

    companion object {
        private val dateFormat = SimpleDateFormat("yyyy-MM-dd")

        private fun Map<String, String>.optString(key: String, def: String = ""): String {
            return this[key] ?: def
        }

        private val String.esc get() = this.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        private val Any?.esc
            get() = when (this) {
                is String -> esc
                null -> ""
                else -> toString().esc
            }

        object ACN835 {
            @Language("HTML")
            fun format(
                cq: ClaimQuery?,
                contextPath: String,
                servletPath: String,
                pathInfo: String,
                value: String): String =
                when {
                    cq?.type?.startsWith("835") == true
                            || (servletPath == "/claim" && pathInfo.startsWith("/835")) ->
                        "<a href='$contextPath/claim/837/${value.urlEnc}?ps=20' target='_blank'>${value.esc}</a>"
                    else ->
                        value.esc
                }
        }

        object NPI {
            private const val uri = "https://npiregistry.cms.hhs.gov/registry/provider-view/"

            @Language("HTML")
            fun format(value: String): String =
                "<a href='$uri${value.urlEnc}' target='_blank'>${value.esc}</a>"
        }

        private val String.urlEnc: String
            get() = URLEncoder.encode(this, "UTF-8")
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
                <strong>${claim["acn"]} | ${claim["prn"].esc}
                    (${claim.amountSpan("clmAsk")}
                    | ${claim.amountSpan("clmPayTotal")}
                    | ${claim.amountSpan("clmPay")}
                    | ${claim.amountSpan("pr")})
                </strong>
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
                is Document -> addDocBody(key, value, context)
                is List<*> -> addArray(key, value, context)
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
                                ${it.valueSpan("adjGrp")} |
                                ${it.valueSpan("adjReason", caption = "Reason: ".graySpan())} |
                                ${it.amountSpan("adjAmt", caption = "Amount: ".graySpan())}
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
                "svc" -> addArrayBodySvc(key, value)
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

    private fun addArrayBodySvc(key: String, value: List<*>) {
        fun addSvcBody(key: String, value: Document, context: ParentContext) {
            addDocBody("", value, context) { doc, ctx ->
                val amountKeys = listOf("cptAsk", "cptAll", "cptPay", "cptPr")
                fun String.toProp(): String? = doc[this]?.let { value ->
                    val caption = formatKey(this, value).esc.graySpan()
                    val text = formatValue(this, value)
                    if (text.isBlank()) null else when (this) {
                        in amountKeys -> "$caption: \$$text"
                        else -> "$caption: $text"
                    }
                }

                val adjKey = "adj"
                val mainKeys = listOf( "cpt", "cptMod1", "qty" ) + amountKeys
                addIndented(mainKeys
                    .mapNotNull { it.toProp() }
                    .joinToString(separator = " | ")
                )
                addCptAdjustments(adjKey, doc[adjKey] as? List<*> ?: emptyList<Any>(), ctx)

                val keysLeft = doc.keys - mainKeys - adjKey
                val arrays = keysLeft
                    .map { it to doc[it] }
                    .filter {it.second is List<*>}
                    .toMap()

                addIndented(
                    (keysLeft - arrays.keys)
                        .mapNotNull { it.toProp() }
                        .joinToString(separator = " | "))

                arrays.forEach { key, list ->
                    withPadding {
                        addArray(key, list as List<*>)
                    }
                }
            }
        }

        value.forEachIndexed { index, item ->
            val context = ParentContext(
                true,
                index == 0,
                value.size == index + 1
            )
            when (item) {
                is Document -> addSvcBody(key, item, context)
                is List<*> -> addArray(key, item, context)
                else -> addArrayScalar(key, item, context)
            }
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

    private fun addDocBody(
        key: String,
        value: Document,
        context: ParentContext = ParentContext.DOC,
        addDocAction: (Document, ParentContext) -> Unit = { doc, ctx -> addDoc(doc, ctx) }
    ) {
        addDocHeader(key, value, context)
        indent++
        addDocAction(value, context)
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
        //language=HTML
        html.append(
            """<body style="font-family: Consolas, monospace; font-size: 14px;">
            <br>
            <br>
            <div style="position: fixed; overflow: auto; top: 0; left: 0; right: 0; background-color: #fff; padding: 5px">
        """
        )
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
            html.append(
                " Records ${paging.pn * paging.ps + 1}-${min(
                    paging.found,
                    (paging.pn + 1) * paging.ps
                )} of ${paging.found} "
            )
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


    private fun keyToHTML(key: String) = key.esc.graySpan()

    private fun addIndented(html: String) {
        //language=HTML
        this.html.append("<div style='padding-left: ${indent}em;'>$html</div>\n")
    }

    private fun withPadding(left: String = "2em", right: String = left, innerHtml: () -> Unit) {
        //language=HTML
        this.html.append("<div style='padding-left: $left; padding-right: $right;'>\n")
        innerHtml()
        //language=HTML
        this.html.append("</div>\n")
    }

    private fun addLink(href: String = "", html: String, separator: String = "\n", fragment: String = "") {
        this.html.append("<a href='$href#$fragment'>$html</a>$separator")
    }

    private val Any?.asHTML: String get() = this?.toString() ?: ""

    @Language("HTML")
    private fun String.graySpan(): String =
        """<span style="color:gray">$esc</span>"""

    @Language("HTML")
    private fun Document.amountSpan(key: String, caption: String = ""): String =
        "${'$'}${this[key].esc}".span(caption, formatKey(key).esc)


    private fun Document.valueSpan(key: String, caption: String = "", default: String = "___"): String {
        val html = this[key]?.let { formatValue(key, it) }
        val title = formatKey(key).esc
        return html.span(caption, title, default)
    }

    @Language("HTML")
    private fun String?.span(
        caption: String = "",
        title: String = "",
        default: String = "___"
    ) = """$caption<span title="$title">${this ?: default}</span>"""

    private fun StringBuilder.claimFrame(inner: () -> Unit) {
        //language=HTML
        append("""<div style="padding-left: 32pt">""")
        inner()
        //language=HTML
        append("""</div>""")
    }

    private fun StringBuilder.arrayFrame(inner: () -> Unit) {
        //language=HTML
        append("""<div style="border: solid darkgrey 1pt; padding-top: 8pt; padding-bottom: 8pt; margin-bottom: 8pt">""")
        inner()
        //language=HTML
        append("""</div>""")
    }
}

