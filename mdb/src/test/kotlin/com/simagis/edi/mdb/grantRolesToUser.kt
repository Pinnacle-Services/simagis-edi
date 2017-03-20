package com.simagis.edi.mdb

import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.simagis.edi.mdb.MDBCredentials
import com.simagis.edi.mdb.grantRolesToUser

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/20/2017.
 */
fun main(args: Array<String>) {
    val host = "mongodb.loc"
    val mongo = MDBCredentials.mongoClient(host)
    mongo.grantRolesToUser("admin", "readWriteAnyDatabase" to "admin")
}
