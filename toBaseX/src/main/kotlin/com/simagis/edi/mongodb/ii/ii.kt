package com.simagis.edi.mongodb.ii

import com.mongodb.client.MongoIterable
import org.bson.Document
import java.util.*

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 1/2/2018.
 */

enum class IIStatus { NEW, RUNNING, SUCCESS, FAILURE }

interface IISession {
    val id: Long
    var status: IIStatus
    var step: String?
    var error: Throwable?
    val files: IIFiles
    var filesFound: Int
    var filesSucceed: Int
    var filesFailed: Int
}

interface IIFiles {
    fun registerFile(file: java.io.File): IIFile
    fun find(status: IIStatus): MongoIterable<IIFile>
}

interface IIFile {
    val id: String
    val sessionId: Long
    val doc: Document
    val status: IIStatus
    val size: Long
    val info: Document?
    val error: Document?
    fun markRunning()
    fun markSucceed(info: Document?)
    fun markFailed(error: Document)
}

interface IIClaims {
    fun findNew(): MongoIterable<IIClaim>
    fun commit()
}

interface IIClaim {
    val valid: Boolean get() = false
    val claim: Document get() = throw AssertionError()
    val type: String get() = "???"
    val date: Date get() = throw AssertionError()
    fun date(claim: Document): Date? = throw AssertionError()
}
