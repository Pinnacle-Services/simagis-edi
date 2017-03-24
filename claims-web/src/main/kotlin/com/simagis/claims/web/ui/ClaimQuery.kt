package com.simagis.claims.web.ui

import com.simagis.claims.rest.api.toJsonObject
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
        private val jsonWriterSettings = JsonWriterSettings(JsonMode.STRICT, true)
        private fun String.toJsonFormatted(): String {
            return Document.parse(this).toJson(jsonWriterSettings)
        }
    }

    val href: String get() = "/claim/$type/=${encode()}?ps=$pageSize"

    private fun toJsonArray(): JsonArray = Json.createArrayBuilder().also { array ->
        array.add(find.toJsonFormatted().toJsonObject())
        array.add(projection.toJsonFormatted().toJsonObject())
        array.add(sort.toJsonFormatted().toJsonObject())
    }.build()

    fun encode(): String = encoder.encodeToString(toJsonArray().toString().toByteArray())

    fun format(): ClaimQuery {
        find = find.toJsonFormatted()
        projection = projection.toJsonFormatted()
        sort = sort.toJsonFormatted()
        return this
    }

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
