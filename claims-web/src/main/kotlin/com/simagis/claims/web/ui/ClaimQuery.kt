package com.simagis.claims.web.ui

import com.simagis.claims.rest.api.ClaimDb
import com.simagis.claims.rest.api.toJsonObject
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

    val href: String get() = "/claim/$type/=${encode()}?ps=$pageSize"

    private fun toJsonArray(): JsonArray = Json.createArrayBuilder().also { array ->
        array.add(find.toJsonFormatted().toJsonObject())
        array.add(projection.toJsonFormatted().toJsonObject())
        array.add(sort.toJsonFormatted().toJsonObject())
    }.build()

    fun encode(): String = encoder.encodeToString(toJsonArray().toString().toByteArray())

    fun toDocument(): Document = Document().apply {
        if (_id != null) append("_id", _id)
        append("name", name)
        append("description", description)
        append("type", type)
        append("find", find)
        append("projection", projection)
        append("sort", sort)
        append("pageSize", pageSize)
        append("created", created)
        append("modified", modified)
    }
}

fun Document.toClaimQuery(): ClaimQuery = ClaimQuery(
        _id = get("_id"),
        name = getString("name") ?: "",
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
                sortOrder.forEach {
                    when (it.sorted.caption) {
                        "Name" -> `+`("name", it.direction.toInt())
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


private val DATE by lazy { SimpleDateFormat("yyyy-MM-dd") }

internal fun Document.applyParameters(request: (String) -> String?): Document = apply {
    fun get(key: String, map: (String?) -> Any?): Any? = map(request(key))
    fun getA(key: String, value: String, map: (String?) -> Any?): Any? = map(request(key)
            ?: value.substringAfter('=', missingDelimiterValue = ""))

    Document(this).forEach { key, value ->
        when {
            value is String && value.startsWith("#") -> {
                when {
                    value == "#" -> this[key] = get(key) { it }
                    value == "#int" -> this[key] = get(key) { it?.toLongOrNull() }
                    value == "#num" -> this[key] = get(key) { it?.toDoubleOrNull() }
                    value == "#cur" -> this[key] = get(key) { it?.toDoubleOrNull() }
                    value == "#date" -> this[key] = get(key) { it?.let { DATE.parse(it) } }
                    value.startsWith("#=") -> this[key] = getA(key, value) { it }
                    value.startsWith("#int=") -> this[key] = getA(key, value) { it?.toLongOrNull() }
                    value.startsWith("#num=") -> this[key] = getA(key, value) { it?.toDoubleOrNull() }
                    value.startsWith("#cur=") -> this[key] = getA(key, value) { it?.toDoubleOrNull() }
                    value.startsWith("#date=") -> this[key] = getA(key, value) { it?.let { DATE.parse(it) } }
                }
            }
            value is Document -> value.applyParameters(request)
            value is List<*> -> value.forEach {
                if (it is Document) it.applyParameters(request)
            }
        }
    }
}
