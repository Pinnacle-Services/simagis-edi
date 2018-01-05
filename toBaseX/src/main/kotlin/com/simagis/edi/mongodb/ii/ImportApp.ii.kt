package com.simagis.edi.mongodb.ii

import com.simagis.edi.mongodb.ImportJob
import com.simagis.edi.mongodb.info
import org.bson.Document
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 12/19/2017.
 */


private val logLock = ReentrantLock()

private fun log(message: String) = logLock.withLock { println(message) }

private fun IISession.step(stepName: String) {
    step = stepName
    log("STEP $stepName")
}

fun main(args: Array<String>) {
    ImportJob.open(args)
    info("starting job", detailsJson = ImportJob.jobDoc)
    val session: IISession = ImportJob.ii.newSession()
    try {
        session.status = IIStatus.RUNNING
        if (ImportJob.options.scanMode == "R") {
            session.step("Scanning source files")
            ImportJob.options.sourceDir.walk().forEach { file ->
                if (file.isFile) session.files.registerFile(file)
            }
        }

        session.step("Finding new files")
        session.files.find(IIStatus.NEW).toList().also { files ->
            session.step("Importing new files")
            ResourceManager().use { executor ->
                files.forEachIndexed { index, file ->
                    executor.call(ImportFileCommand(file)) { result ->
                        val message = """${result.javaClass.simpleName} "${file.doc["names"]}" $index [${files.size}]"""
                        when (result) {
                            is CommandSuccess -> {
                                val info: Document? = result.doc["info"] as? Document
                                log("$message: $info")
                                file.markSucceed(info)
                            }
                            is CommandError -> {
                                val error: Document = result.error
                                log("$message: ${error["message"]}")
                                file.markFailed(error)
                            }
                        }
                    }
                }
            }
        }

        session.step("Adding new claims")
        ImportJob.ii.getClaims().also { claims: IIClaims ->
            val options = AllClaimsUpdateChannel.Options(
                    sessionId = session.id,
                    dateAfter = ImportJob.options.after)
            AllClaimsUpdateChannel(options).also { channel ->
                claims.findNew().forEach { channel.put(it) }
                channel.shutdownAndWait()
                claims.commit()
            }
        }

        session.step("Adding new 835c claims")
//        ImportJob.ii.claims.openCollection(ImportJob.ii.claims.ClaimType.`835`).also { claims: IIClaims ->
//            Claims835cUpdateChannel().also { channel ->
//                claims.findNew().forEach { channel.put(it) }
//                channel.shutdownAndWait()
//                claims.commit()
//            }
//        }

        session.step("Closing session")
        session.status = IIStatus.SUCCESS
    } catch (e: Throwable) {
        session.status = IIStatus.FAILURE
        session.error = e
        e.printStackTrace()
    }
}