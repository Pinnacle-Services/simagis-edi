package com.simagis.edi.basex

import com.microsoft.sqlserver.jdbc.SQLServerDataSource
import org.apache.commons.dbutils.DbUtils
import org.apache.commons.dbutils.QueryRunner
import java.io.Closeable
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.security.MessageDigest
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/6/2017.
 */
class XReg {
    private val qr: QueryRunner by lazy { QueryRunner(SQLServerDataSource().open()) }

    fun newSession(uuid: String? = null, block: XSession.() -> Unit) {
        XSession(this, uuid).use { it.block() }
    }

    class XSession(xReg: XReg, uuid: String?) : Closeable {
        internal val qr = xReg.qr
        val id: Int
        val uuid = uuid ?: UUID.randomUUID().toString()
        val started: LocalDateTime = LocalDateTime.now()
        val xLog = XLog(this)

        init {
            //language=TSQL
            id = qr.insert(
                    "INSERT INTO SESSION (UUID, STARTED) VALUES (?, ?)",
                    {
                        it.next()
                        it.getInt(1)
                    },
                    arrayOf(this.uuid, Timestamp.valueOf(started))
            )
        }

        fun withFile(file: File, block: XFile.() -> Unit) {
            context(XFile.of(this, file), block)
        }

        private fun context(xFile: XFile, block: XFile.() -> Unit) {
            val xContext = xLog.xContext
            try {
                xLog.xContext = XContext(this, xFile)
                xFile.block()
            } finally {
                xLog.xContext = xContext
            }
        }

        override fun close() {
            //language=TSQL
            qr.update("UPDATE SESSION SET FINISHED = ?",
                    *arrayOf(Timestamp.valueOf(LocalDateTime.now()))
            )
        }

        fun forEachInputFile(function: (File) -> Unit) {
            XInput.list(this).forEach { input -> input.forEachFile(function) }
        }
    }

    private class XInput(val PATH: String, val MODE: String) {
        companion object {
            fun list(session: XReg.XSession): List<XInput> = session.qr
                    .query(/* language=TSQL */ "SELECT PATH, MODE FROM INPUT", {
                        mutableListOf<XInput>().apply {
                            while (it.next()) {
                                this += XInput(it.getString(1), it.getString(2))
                            }
                        }
                    })
        }

        fun forEachFile(block: (File) -> Unit): Unit {
            when (MODE) {
                "R" -> File(PATH).walk().forEach { if (it.isFile) forFile(it, block) }
                "D" -> File(PATH).listFiles()?.forEach { if (it.isFile) forFile(it, block) }
                "F" -> forFile(File(PATH), block)
            }
        }

        private fun forFile(file: File, block: (File) -> Unit) {
            block(file)
        }
    }

    class XFile private constructor(
            internal val xSession: XSession,
            val fileID: Long,
            val fileDigest: String,
            val filePath: String,
            val fileName: String,
            fileStatus: String
    ) {
        private var fileStatus_ = fileStatus
        val fileStatus: String get() = fileStatus_

        fun updateFileStatus(status: String? = null) {
            if (status != null) {
                when (status) {
                    "REGISTERED" -> xSession.qr.update(// language=TSQL
                            "UPDATE [FILE] SET STATUS = ?, REGISTERED = ? WHERE ID = ?",
                            *arrayOf(status, Timestamp.valueOf(LocalDateTime.now()), fileID))
                    else -> xSession.qr.update(// language=TSQL
                            "UPDATE [FILE] SET STATUS = ? WHERE ID = ?",
                            *arrayOf(status, fileID))
                }
                fileStatus_ = status
            }
        }

        fun split(): List<ISA> {
            try {
                return ISA.read(File(filePath))
            } catch(e: Throwable) {
                xSession.xLog.warning(
                        message = "ISA.read(File($filePath)) error",
                        action = "ISA.read",
                        exception = e
                )
                updateFileStatus("INVALID")
                throw e
            }
        }

        fun withISA(isa: ISA, block: XISA.() -> Unit) {
            context(XISA.of(this, isa), block)
        }

        private fun context(xISA: XISA, block: XISA.() -> Unit) {
            val xContext = xSession.xLog.xContext
            try {
                xSession.xLog.xContext = xContext.copy(xISA = xISA)
                xISA.block()
            } finally {
                xSession.xLog.xContext = xContext
            }
        }

        companion object {
            fun of(xSession: XSession, file: File): XFile {
                val id: Long
                val digest = md.digest(file.readBytes()).toHexString()
                val path = file.absolutePath
                val name = file.name

                val qr = xSession.qr

                class REC(val ID: Long, val STATUS: String)
                //language=TSQL
                val rec = qr.query(
                        "SELECT ID, STATUS FROM [FILE] WHERE DIGEST = ?",
                        { if (it.next()) REC(it.getLong(1), it.getString(2)) else null },
                        arrayOf(digest))

                var isNewPathAlreadyExists = false
                if (rec != null) {
                    id = rec.ID
                    //language=TSQL
                    isNewPathAlreadyExists = qr.query(
                            "SELECT * FROM FILE_PATH WHERE FILE_ID = ? AND PATH = ?",
                            ResultSet::next, arrayOf(id, path))
                } else {
                    //language=TSQL
                    id = qr.insert(
                            "INSERT INTO [FILE] (DIGEST) VALUES (?)",
                            { it.next(); it.getLong(1) },
                            arrayOf<Any?>(digest))
                }

                if (!isNewPathAlreadyExists) {
                    //language=TSQL
                    qr.insert(
                            "INSERT INTO FILE_PATH (FILE_ID, PATH, NAME) VALUES (?, ?, ?)",
                            {},
                            arrayOf(id, path, name))
                }
                return XFile(xSession, id, digest, path, name, rec?.STATUS ?: "")
            }
        }
    }

    class XISA private constructor(
            internal val xSession: XSession,
            val isaID: Long,
            val isaDigest: String,
            isaStatus: String
    ) {
        private var isaStatus_ = isaStatus
        val isaStatus: String get() = isaStatus_

        fun updateIsaStatus(status: String? = null) {
            if (status != null) {
                //language=TSQL
                xSession.qr.update("UPDATE ISA SET STATUS = ? WHERE ID = ?", *arrayOf(status, isaID))
                isaStatus_ = status
            }
        }

        companion object {
            fun of(xFile: XFile, isa: ISA): XISA {
                val id: Long
                val digest = md.digest(isa.code.toByteArray(ISA.CHARSET)).toHexString()
                val qr = xFile.xSession.qr

                class REC(val ID: Long, val STATUS: String)
                //language=TSQL
                val rec = qr.query(
                        "SELECT ID, STATUS FROM ISA WHERE DIGEST = ?",
                        { if (it.next()) REC(it.getLong(1), it.getString(2)) else null },
                        arrayOf(digest))


                var isIsaIdAlreadyInR = false
                if (rec != null) {
                    id = rec.ID
                    //language=TSQL
                    isIsaIdAlreadyInR = qr.query(
                            "SELECT * FROM R_ISA_FILE WHERE ISA_ID = ? AND FILE_ID = ?",
                            ResultSet::next,
                            arrayOf(rec.ID, xFile.fileID))
                } else {
                    //language=TSQL
                    id = qr.insert(
                            "INSERT INTO ISA (DIGEST, DOC_TYPE, DATE8, TIME4) VALUES (?,?,?,?)",
                            { it.next(); it.getLong(1) },
                            arrayOf<Any?>(
                                    digest,
                                    isa.stat.doc.type,
                                    isa.stat.doc.date,
                                    isa.stat.doc.time
                            ))
                }

                if (!isIsaIdAlreadyInR) {
                    //language=TSQL
                    qr.insert(
                            "INSERT INTO R_ISA_FILE (ISA_ID, FILE_ID) VALUES (?, ?)",
                            {},
                            arrayOf(id, xFile.fileID))
                }

                return XISA(xFile.xSession, id, digest, rec?.STATUS ?: "")
            }
        }
    }

    companion object {
        private val md: MessageDigest get() = MessageDigest.getInstance("SHA")

        private fun SQLServerDataSource.open(): DataSource {
            serverName = properties["serverName"]
            portNumber = properties["portNumber"].toInt()
            instanceName = properties["instanceName"]
            databaseName = properties["databaseName"]
            user = properties["user"]
            setPassword(properties["password"])
            return this
        }

        private val properties: Properties by lazy {
            Properties().apply {
                if (!propertiesFile.exists()) {
                    XReg::class.java.getResourceAsStream(propertiesFile.name).use { defaults ->
                        propertiesFile.outputStream().use { defaults.copyTo(it) }
                    }
                }
                propertiesFile.inputStream().use { load(it) }
            }
        }

        private val propertiesFile =
                File(System.getenv("USERPROFILE") ?: ".").resolve(".x-reg.properties")

        private operator fun Properties.get(name: String): String = getProperty(name, null)
                ?: throw SQLException("""property "$name" not found in $propertiesFile""")

        private fun ByteArray.toHexString(): String = joinToString(separator = "") {
            Integer.toHexString(it.toInt() and 0xff)
        }

        private fun QueryRunner.tx(block: QueryRunner.(Connection) -> Unit) {
            val connection: Connection = dataSource.connection
            try {
                block(connection)
                DbUtils.commitAndClose(connection)
            } catch(e: Throwable) {
                try {
                    DbUtils.rollbackAndClose(connection)
                } catch(r: SQLException) {
                    throw r.apply { nextException = SQLException(e) }
                }
                throw e
            }
        }
    }

    data class XContext(
            val xSession: XSession? = null,
            val xFile: XFile? = null,
            val xISA: XISA? = null
    )

    class XLog(private val xSession: XSession) {
        var xContext = XContext()

        fun trace(message: String, action: String? = null, details: String? = null, detailsXml: String? = null) {
            log(Level.TRACE, message, action, details, detailsXml)
        }

        fun info(message: String, action: String? = null, details: String? = null, detailsXml: String? = null) {
            log(Level.INFO, message, action, details, detailsXml)
        }

        fun warning(message: String, exception: Throwable? = null, action: String? = null, details: String? = null, detailsXml: String? = null) = log(
                level = Level.WARNING,
                message = message,
                action = action,
                details = details(details, exception),
                detailsXml = detailsXml
        )

        fun error(message: String, exception: Throwable? = null, action: String? = null, details: String? = null, detailsXml: String? = null) = log(
                level = Level.ERROR,
                message = message,
                action = action,
                details = details(details, exception),
                detailsXml = detailsXml
        )

        private fun details(details: String?, exception: Throwable?): String? = details ?: exception?.let {
            StringWriter().apply { it.printStackTrace(PrintWriter(this)) }.toString()
        }

        private fun log(level: Level, message: String, action: String? = null, details: String? = null, detailsXml: String? = null) {
            //language=TSQL
            xSession.qr.insert("""
                    INSERT INTO LOG (
                            LEVEL,
                            SESSION_ID,
                            FILE_ID,
                            ISA_ID,
                            MESSAGE,
                            ACTION,
                            DETAILS,
                            DETAILS_XML
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    {},
                    arrayOf(
                            level.value,
                            xContext.xSession?.id,
                            xContext.xFile?.fileID,
                            xContext.xISA?.isaID,
                            message,
                            action,
                            details,
                            detailsXml
                    ))
        }

        private enum class Level(val value: Int) {
            TRACE(100),
            INFO(500),
            WARNING(1000),
            ERROR(5000),
        }
    }
}