package com.simagis.edi.mdb

import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import org.bson.Document

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/8/2017.
 */

fun mongoClient(host: String): MongoClient = MongoClient(ServerAddress(host))

fun MongoClient.grantRolesToUser(user: String, vararg roles: Pair<String, String>) {
    val database = getDatabase("admin")
    database.runCommand(Document("grantRolesToUser", user)
            .append("roles", roles.map {
                Document()
                        .append("role", it.first)
                        .append("db", it.second)
            }.toList()))
}
