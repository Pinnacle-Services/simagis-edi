package com.simagis.edi.mdb

import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import org.bson.Document

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/8/2017.
 */

fun mongoClient(host: String): MongoClient = MongoClient(ServerAddress(host))

fun MongoClient.grantRolesToUser(user: String, vararg roleToDBs: Pair<String, String>) {
    val database = getDatabase("admin")
    database.runCommand(doc {
        `+`("grantRolesToUser", user)
        `+`("roles", roleToDBs.map { doc { `+`("role" to it.first, "db" to it.second) } }.toList())
    })
}

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

@Suppress("unused")
inline fun Document.`+$set`(build: Document.() -> Unit): Document {
    append(`$` + "set", Document().apply {
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

fun Document.`+`(pair: Pair<String, Any?>, vararg pairs: Pair<String, Any?>): Document {
    append(pair.first, pair.second)
    pairs.forEach { append(it.first, it.second) }
    return this
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
