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
