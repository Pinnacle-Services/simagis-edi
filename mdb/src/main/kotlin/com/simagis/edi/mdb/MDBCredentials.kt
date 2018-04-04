package com.simagis.edi.mdb

import com.mongodb.MongoClient
import com.mongodb.ServerAddress

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/20/2017.
 */
object MDBCredentials {
    fun mongoClient(address: ServerAddress): MongoClient = MongoClient(address)
}

