package com.simagis.edi.mongodb

import com.mongodb.ServerAddress
import com.simagis.edi.basex.get
import com.simagis.edi.mdb.MDBCredentials
import com.simagis.edi.mdb.`+$gt`
import com.simagis.edi.mdb.doc
import java.sql.Date

/**
 *
 * Created by avylegzhanin12897 on 5/15/2017.
 */

fun main(args: Array<String>) {
    val commandLine = com.berryworks.edireader.util.CommandLine(args)
    val mongo = MDBCredentials.mongoClient(ServerAddress(
        commandLine["host"] ?: ServerAddress.defaultHost(),
        commandLine["port"]?.toInt() ?: ServerAddress.defaultPort()
    ))
    val claimsAPI = mongo.getDatabase("claimsAPI")
    val apiJobsLog = claimsAPI.getCollection("apiJobsLog")

    val filter = doc {
        `+$gt`("level", 500)
        `+$gt`("time", Date.valueOf("2017-05-01"))
    }
    val dbCursor = apiJobsLog.find(filter)
    var invalidFile = 0
    var invalidISA = 0
    dbCursor.forEach {
        val msg = it["msg"] as? String
        if (msg != null) {
            when {
                msg.startsWith("Invalid file ") -> invalidFile++
                msg.startsWith("Invalid ISA: ") -> invalidISA++
                msg.startsWith("restartRequired: memory =") -> {}
                msg.startsWith("RESTARTING for ") -> {}
                msg.startsWith("temp collection already exists -> ") -> {}
                else -> {
                    println(msg)
                    (it["msg"] as? String)?.let {  }
                }
            }
        }
    }

    println("** Invalid file: $invalidFile")
    println("** Invalid ISA: $invalidISA")
}