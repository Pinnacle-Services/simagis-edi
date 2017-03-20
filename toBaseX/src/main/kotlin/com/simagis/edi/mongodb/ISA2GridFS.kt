package com.simagis.edi.mongodb

import com.mongodb.DB
import com.mongodb.MongoClient
import com.mongodb.gridfs.GridFS
import com.simagis.edi.basex.ISA
import com.simagis.edi.basex.get
import java.io.File
import java.security.MessageDigest

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/20/2017.
 */
fun main(args: Array<String>) {
    val commandLine = com.berryworks.edireader.util.CommandLine(args)
    val _host = commandLine["host"] ?: "localhost"
    val _fs = commandLine["fs"] ?: "isa"
    val _mode = commandLine["mode"] ?: "R"
    val _path = File(commandLine[0])

    val mongo = MongoClient(_host)
    val db = DB(mongo, _fs)

    val fs = GridFS(db)

    fun File.walk(mode: String): List<File> {
        return when (mode) {
            "R" -> walk().toList()
            "D" -> listFiles()?.asList() ?: emptyList()
            "F" -> listOf(this)
            else -> emptyList()
        }.filter {
            it.isFile
        }
    }

    fun md(): MessageDigest = MessageDigest.getInstance("SHA")
    fun ByteArray.toHexString(): String = joinToString(separator = "") {
        Integer.toHexString(it.toInt() and 0xff)
    }

    _path.walk(_mode).forEach {
        try {
            ISA.read(it).forEach { isa ->
                val byteArray = isa.code.toByteArray(ISA.CHARSET)
                val digest = md().digest(byteArray).toHexString()
                fs.createFile(byteArray.inputStream(), "/isa/$digest").save()
            }
        } catch(e: Exception) {
            e.printStackTrace()
        }
    }
}