package com.simagis.edi.mongodb.dictionary

import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import javax.json.Json
import javax.json.JsonObject

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 7/14/2017.
 */

interface DictionaryContext {
    val name: String
    val db: MongoDatabase
    val builder: DictionaryBuilder
    val collection: MongoCollection<Document> get() = db.getCollection(name)
}

interface DictionaryBuilder {
    fun init(context: DictionaryContext)
    fun collect(claim: Document)
    fun save()
}

data class DictionaryItem(val _id: String, val dsc: String, val search: String, val inUse: Boolean)

fun Document.toItem(): DictionaryItem? {
    return DictionaryItem(
            _id = this["_id"] as? String ?: return null,
            dsc = this["dsc"] as? String ?: return null,
            search = this["search"] as? String ?: return null,
            inUse = this["inUse"] as? Boolean ?: return null
    )
}

fun DictionaryItem.toDocument() = Document().apply {
    this["_id"] = _id
    this["dsc"] = dsc
    this["search"] = search
    this["inUse"] = inUse
}

val String.bson: Document get() = Document.parse(this)

typealias CodesMap = Map<String, String>
typealias ItemList = List<DictionaryItem>

fun Class<*>.readCodesMap(name: String, map: (JsonObject) -> Pair<String, String>?): CodesMap = mutableMapOf<String, String>().apply {
    getResourceAsStream(name).use { Json.createReader(it).readArray() }.forEach {
        if (it is JsonObject) {
            map(it)?.let { this += it }
        }
    }
}

fun CodesMap.toItemBuilder(): (String) -> DictionaryItem {
    return { id ->
        val dsc = this[id]
        when {
            dsc != null -> DictionaryItem(id, dsc, "$id-$dsc", true)
            else -> id.toDictionaryItem(true)
        }
    }
}

fun MongoCollection<Document>.insertAll(items: ItemList) = items.forEach { item ->
    insertOne(item.toDocument())
}

fun MongoCollection<Document>.updateInUse(items: List<DictionaryItem>) = items.forEach { item ->
    val filter = Document("\$eq", Document("_id", item._id))
    val update = Document("\$set", Document("inUse", item.inUse))
    updateOne(filter, update)
}

fun MongoCollection<Document>.insertAll(codes: CodesMap) = codes.forEach { _id, dsc ->
    insertOne(Document().apply {
        append("_id", _id)
        append("dsc", dsc)
        append("search", """$_id-$dsc""")
        append("inUse", true)
    })
}

fun MongoCollection<Document>.replaceAll(codes: CodesMap) {
    deleteMany("{}".bson)
    insertAll(codes)
}

fun String.toDictionaryItem(inUse: Boolean) = DictionaryItem(this, this, this, inUse)

inline fun <reified T> Document?.opt(name: String): T? = this?.get(name) as? T

inline val Any?.doc: Document? get() = this as? Document
