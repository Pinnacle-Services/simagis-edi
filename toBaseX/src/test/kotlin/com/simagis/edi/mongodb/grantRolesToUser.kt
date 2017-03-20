package com.simagis.edi.mongodb

import com.mongodb.MongoClient
import com.mongodb.ServerAddress

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/20/2017.
 */
fun main(args: Array<String>) {
    val host = "mongodb.loc"
    val mongo = MongoClient(ServerAddress(host), MDBCredentials[host])
    mongo.grantRolesToUser("admin", "readWriteAnyDatabase" to "admin")
}
