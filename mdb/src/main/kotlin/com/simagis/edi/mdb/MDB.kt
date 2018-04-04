package com.simagis.edi.mdb

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/8/2017.
 */

fun MongoClient.grantRolesToUser(user: String, vararg roleToDBs: Pair<String, String>) {
    val database = getDatabase("admin")
    database.runCommand(doc {
        `+`("grantRolesToUser", user)
        `+`("roles", roleToDBs.map { doc { `+`("role" to it.first, "db" to it.second) } }.toList())
    })
}

operator fun MongoDatabase.get(collectionName: String): MongoCollection<Document> = getCollection(collectionName)


@Suppress("unused")
object `$` {
    override fun toString(): String = '$'.toString()
    operator fun invoke(name: String): String = '$' + name
    operator fun plus(name: String): String = '$' + name
}

@Suppress("NOTHING_TO_INLINE")
inline fun Document.`+`(key: String, value: Any?): Document = append(key, value)

@Suppress("NOTHING_TO_INLINE", "unused")
inline fun Document.`+$`(key: String, value: Any?): Document = append(`$`(key), value)

@Suppress("NOTHING_TO_INLINE", "unused")
inline fun Document.`+$gt`(key: String, value: Any?): Document = append(key, Document(`$`("gt"), value))

@Suppress("NOTHING_TO_INLINE", "unused")
inline fun Document.`+$gte`(key: String, value: Any?): Document = append(key, Document(`$`("gte"), value))

@Suppress("NOTHING_TO_INLINE", "unused")
inline fun Document.`+$lt`(key: String, value: Any?): Document = append(key, Document(`$`("lt"), value))

@Suppress("NOTHING_TO_INLINE", "unused")
inline fun Document.`+$lte`(key: String, value: Any?): Document = append(key, Document(`$`("lte"), value))

@Suppress("NOTHING_TO_INLINE", "unused")
inline fun Document.`+$regex`(regex: String): Document = append(`$`("regex"), regex)

@Suppress("NOTHING_TO_INLINE", "unused")
inline fun Document.`+$options`(options: String): Document = append(`$`("options"), options)

@Suppress("unused")
inline fun Document.`+$set`(build: Document.() -> Unit): Document {
    append(`$` + "set", Document().apply {
        build()
    })
    return this
}

@Suppress("unused")
inline fun Document.`+$unset`(build: Document.() -> Unit): Document {
    append(`$` + "unset", Document().apply {
        build()
    })
    return this
}

@Suppress("unused")
inline fun Document.`+$setOnInsert`(build: Document.() -> Unit): Document {
    append(`$` + "setOnInsert", Document().apply {
        build()
    })
    return this
}

@Suppress("unused")
inline fun Document.`+$addToSet`(build: Document.() -> Unit): Document {
    append(`$` + "addToSet", Document().apply {
        build()
    })
    return this
}

@Suppress("unused")
inline fun Document.`+$addToSet++`(build: Document.() -> Unit): Document {
    val addToSet = get(`$` + "addToSet") as? Document
    if (addToSet != null)
        addToSet.build()
    else
        `+$addToSet`(build)
    return this
}

@Suppress("unused")
inline fun Document.`+$inc`(build: Document.() -> Unit): Document {
    append(`$` + "inc", Document().apply {
        build()
    })
    return this
}

fun Document.`+`(pair: Pair<String, Any?>, vararg pairs: Pair<String, Any?>): Document {
    append(pair.first, pair.second)
    pairs.forEach { append(it.first, it.second) }
    return this
}

fun updater(pairs: Array<out Pair<String, Any?>>): Document {
    val sets = mutableMapOf<String, Any>()
    val unsets = mutableSetOf<String>()
    pairs.forEach { pair ->
        with(pair) {
            if (second != null) sets[first] = second!! else unsets += first
        }
    }
    return doc {
        if (sets.isNotEmpty()) `+$set` {
            sets.forEach { name, value -> `+`(name, value) }
        }
        if (unsets.isNotEmpty()) `+$unset` {
            unsets.forEach { name -> `+`(name, "") }
        }
    }
}

inline fun doc(build: Document.() -> Unit) = Document().apply { build() }
@Suppress("NOTHING_TO_INLINE")
inline fun doc(_id: Any?) = Document("_id", _id)

inline fun doc(_id: Any?, build: Document.() -> Unit) = Document("_id", _id).apply { build() }

val Document._id: Any? get() = this["_id"]

operator fun Document?.invoke(vararg keyPath: String): Any? {
    var res: Any? = this
    for (key in keyPath) {
        res = if (res is Document) res[key] else return null
    }
    return res
}
