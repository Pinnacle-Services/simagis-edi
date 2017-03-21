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

object `$` {
    override fun toString(): String = '$'.toString()
    operator fun invoke(name: String): String = '$' + name
}

@Suppress("NOTHING_TO_INLINE")
inline fun Document.`+`(key: String, value: Any?): Document = append(key, value)

fun Document.`+`(pair: Pair<String, Any?>, vararg pairs: Pair<String, Any?>): Document {
    append(pair.first, pair.second)
    pairs.forEach { append(it.first, it.second) }
    return this
}

inline fun doc(build: Document.() -> Unit) = Document().apply { build() }