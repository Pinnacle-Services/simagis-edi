package com.simagis.claims.web.ui

import com.simagis.claims.rest.api.ClaimDb
import com.simagis.claims.rest.api.toJsonObject
import com.simagis.edi.mdb.`+$options`
import com.simagis.edi.mdb.`+$regex`
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import com.vaadin.data.provider.QuerySortOrder
import com.vaadin.shared.data.sort.SortDirection
import com.vaadin.ui.Grid
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.text.SimpleDateFormat
import java.util.*
import javax.json.Json
import javax.json.JsonArray

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/24/2017.
 */
data class ClaimQuery(
        var _id: Any? = null,
        var name: String = "New Claim Query",
        var path: String = UUID.randomUUID().toString(),
        var description: String = "",
        var type: String = "835",
        var find: String = "{}",
        var projection: String = "{}",
        var sort: String = "{}",
        var pageSize: Int = 20,
        var created: Date = Date(),
        var modified: Date = Date()
) {
    companion object {
        private val encoder: Base64.Encoder = Base64.getUrlEncoder()
    }

    private fun toJsonArray(): JsonArray = Json.createArrayBuilder().also { array ->
        array.add(find.toJsonFormatted().toJsonObject())
        array.add(projection.toJsonFormatted().toJsonObject())
        array.add(sort.toJsonFormatted().toJsonObject())
    }.build()

    fun encode(): String = encoder.encodeToString(toJsonArray().toString().toByteArray())

    fun toDocument(): Document = Document().apply {
        if (_id != null) append("_id", _id)
        append("name", name)
        append("path", path)
        append("description", description)
        append("type", type)
        append("find", find)
        append("projection", projection)
        append("sort", sort)
        append("pageSize", pageSize)
        append("created", created)
        append("modified", modified)
    }

    fun toParameters(): Set<Parameter> = Document.parse(find).toParameters()

    data class Parameter(val name: String, val operator: String, val default: String? = null)
}

fun Document.toClaimQuery(): ClaimQuery = ClaimQuery(
        _id = get("_id"),
        name = getString("name") ?: "",
        path = getString("path") ?: "",
        description = getString("description") ?: "",
        type = getString("type") ?: "Invalid",
        find = getString("find") ?: "{}",
        projection = getString("projection") ?: "{}",
        sort = getString("sort") ?: "{}",
        pageSize = getInteger("pageSize") ?: 20,
        created = getDate("created") ?: Date(),
        modified = getDate("modified") ?: Date()
)

private val jsonWriterSettings = JsonWriterSettings(JsonMode.STRICT, true)

fun String.toJsonFormatted(): String {
    return Document.parse(this).toJson(jsonWriterSettings)
}

fun Grid<ClaimQuery>.refresh() = setDataProvider(
        { _: List<QuerySortOrder>, offset, limit ->
            val sort = doc {
                fun SortDirection.toInt(): Int = when (this) {
                    SortDirection.DESCENDING -> 1
                    SortDirection.ASCENDING -> -1
                }
                if (sortOrder.isEmpty()) `+`("name", 1)
                sortOrder.forEach {
                    when (it.sorted.caption) {
                        "Name" -> `+`("name", it.direction.toInt())
                        "Path" -> `+`("path", it.direction.toInt())
                        "Type" -> `+`("type", it.direction.toInt())
                        "Created" -> `+`("created", it.direction.toInt())
                        "Modified" -> `+`("modified", it.direction.toInt())
                        "page size" -> `+`("pageSize", it.direction.toInt())
                    }
                }
            }
            ClaimDb.cq.find()
                    .sort(sort)
                    .skip(offset)
                    .limit(limit)
                    .toList()
                    .map { it.toClaimQuery() }
                    .stream()
        },
        {
            ClaimDb.cq.find().count()
        })


private val DATE4 by lazy { SimpleDateFormat("yyyy") }
private val DATE7 by lazy { SimpleDateFormat("yyyy-MM") }
private val DATE10 by lazy { SimpleDateFormat("yyyy-MM-dd") }

private fun String.toParameter(): ClaimQuery.Parameter? {
    if (!startsWith("#")) return null
    val icn = indexOf(':')
    val ieq = indexOf('=')
    val name = when {
        icn == -1 && ieq == -1 -> substring(1)
        icn != -1 && ieq == -1 -> substring(1, icn)
        icn == -1 && ieq != -1 -> substring(1, ieq)
        icn != -1 && ieq != -1 -> substring(1, icn)
        else -> ""
    }
    if (name.isBlank()) return null

    val operator = when {
        icn != -1 && ieq != -1 -> substring(icn + 1, ieq)
        icn != -1 && ieq == -1 -> substring(icn + 1)
        else -> ""
    }
    val default: String? = when {
        ieq != -1 -> substring(ieq + 1)
        else -> null
    }

    return ClaimQuery.Parameter(name, operator, default)
}

private fun Document.toParameters(): Set<ClaimQuery.Parameter>
        = mutableMapOf<String, ClaimQuery.Parameter>()
        .apply map@ {
            fun Document.searchParameters() {
                values.forEach {
                    when (it) {
                        is String -> it.toParameter()?.let {
                            this@map[it.name] = it
                        }
                        is Document -> it.searchParameters()
                        is List<*> -> it.forEach {
                            if (it is Document) it.searchParameters()
                        }
                    }
                }
            }
            this@toParameters.searchParameters()
        }
        .toSortedMap().values.toSet()

internal fun Document.applyParameters(request: (String) -> String?): Document = apply {
    Document(this).forEach { key, value ->
        when (value) {
            is String -> value.toParameter()?.let { (name, operator, default) ->

                fun apply(map: (String?) -> Any?) {
                    val mapped = map(request(name) ?: default)
                    if (mapped != null)
                        this[key] = mapped else
                        this.remove(key)
                }

                fun String.toRegex(options: String): Document = doc {
                    `+$regex`(this@toRegex)
                    `+$options`(options)
                }

                fun String.toDate(): Date? = when(length) {
                    10 -> DATE10.parse(this)
                    7 -> DATE7.parse(this)
                    4 -> DATE4.parse(this)
                    else -> null
                }

                when (operator) {
                    "" -> apply { it }
                    "int" -> apply { it?.toLongOrNull() }
                    "num" -> apply { it?.toDoubleOrNull() }
                    "cur" -> apply { it?.toDoubleOrNull() }
                    "date" -> apply { it?.toDate() }
                    "contains" -> apply { it?.let { ".*$it.*".toRegex("i") } }
                    "contains/" -> apply { it?.let { ".*$it.*".toRegex("") } }
                    "startsWith" -> apply { it?.let { "^$it.*".toRegex("i") } }
                    "startsWith/" -> apply { it?.let { "^$it.*".toRegex("") } }
                }
            }
            is Document -> value.applyParameters(request)
            is List<*> -> value.forEach {
                if (it is Document) it.applyParameters(request)
            }
        }
    }
}
