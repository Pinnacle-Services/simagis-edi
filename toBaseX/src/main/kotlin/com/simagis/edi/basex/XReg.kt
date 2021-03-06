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
import java.time.Instant
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/6/2017.
 */
class XReg {
    private val qr: QueryRunner by lazy { QueryRunner(SQLServerDataSource().open()) }

    fun newSession(uuid: String? = null, block: XSession.() -> Unit) {
        XSession(this, uuid).use {
            it.status = "STARTING"
            try {
                it.block()
                it.status = "FINISHED"
            } catch(e: Exception) {
                it.status = "FAILED"
                throw e
            }
        }
    }

    class XSession(xReg: XReg, uuid: String?) : Closeable {
        internal val qr = xReg.qr
        val id: Int
        val uuid = uuid ?: UUID.randomUUID().toString()
        val started: LocalDateTime = LocalDateTime.now()

        // language=TSQL
        var status: String by UpdateSessionByID("", "UPDATE SESSION SET STATUS = ?, STATUS_TIME = ? WHERE ID = ?")
        // language=TSQL
        var filesIn: Int by UpdateSessionByID(0, "UPDATE SESSION SET FILES_IN = ?, STATUS_TIME = ? WHERE ID = ?")
        // language=TSQL
        var filesDone: Int by UpdateSessionByID(0, "UPDATE SESSION SET FILES_DONE = ?, STATUS_TIME = ? WHERE ID = ?")
        // language=TSQL
        var optimizationsIn: Int by UpdateSessionByID(0, "UPDATE SESSION SET OPTIMIZATIONS_IN = ?, STATUS_TIME = ? WHERE ID = ?")
        // language=TSQL
        var optimizationsDone: Int by UpdateSessionByID(0, "UPDATE SESSION SET OPTIMIZATIONS_DONE = ?, STATUS_TIME = ? WHERE ID = ?")

        private class UpdateSessionByID<T>(var value: T, val sql: String) : ReadWriteProperty<XReg.XSession, T> {
            override fun getValue(thisRef: XReg.XSession, property: KProperty<*>): T = value
            override fun setValue(thisRef: XReg.XSession, property: KProperty<*>, value: T) {
                thisRef.qr.update(sql, *arrayOf(value, Timestamp.from(Instant.now()), thisRef.id))
                this.value = value
            }
        }

        internal val xIsaMap: MutableMap<String, XISA> by lazy {
            mutableMapOf<String, XISA>().apply {
                xLog.info("Loading ISA table...")
                qr.query(/* language=TSQL */ "SELECT ID, DIGEST, STATUS FROM ISA") {
                    while (it.next()) {
                        val xIsa = XISA(
                                xSession = this@XSession,
                                isaID = it.getLong(1),
                                isaDigest = it.getString(2).trim(),
                                isaStatus = it.getString(3).trim())
                        this[xIsa.isaDigest] = xIsa
                    }
                }

                xLog.info("Loading R_ISA_FILE table...")
                qr.query(/* language=TSQL */ """
                    SELECT ISA.DIGEST, [FILE].DIGEST
                        FROM R_ISA_FILE
                        INNER JOIN ISA ON ISA_ID = ISA.ID
                        INNER JOIN [FILE] ON FILE_ID = [FILE].ID
                    """) { record ->
                    while (record.next()) {
                        this[record.getString(1).trim()]
                                ?.isaFiles
                                ?.add(record.getString(2).trim())
                    }
                }
                xLog.info("Loading of tables completed")
            }
        }

        val xLog: XLog get() = xLogDB

        private val xLogDB = XLogDB(this)
        internal var xContext: XContext
            get() = xLogDB.xContext
            set(value) {
                xLogDB.xContext = value
            }

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
            val xContext = this.xContext
            try {
                this.xContext = XContext(this, xFile)
                xFile.block()
            } finally {
                this.xContext = xContext
            }
        }

        override fun close() {
            //language=TSQL
            qr.update("UPDATE SESSION SET FINISHED = ? WHERE ID = ?",
                    *arrayOf(Timestamp.valueOf(LocalDateTime.now()), id)
            )
        }

        fun listInputFiles(): List<File> = XInput
                .list(this)
                .flatMap { it.listFiles() }

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

        fun listFiles(): List<File> = when (MODE) {
            "R" -> File(PATH).walk().toList()
            "D" -> File(PATH).listFiles()?.asList() ?: emptyList()
            "F" -> listOf(File(PATH))
            else -> emptyList()
        }.filter {
            it.isFile
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
        val asFile = File(filePath)
        var fileStatus: String = fileStatus
            set(value) {
                updateFileStatus(value)
                field = value
            }

        private fun updateFileStatus(status: String): Unit {
            when (status) {
                "REGISTERED" -> xSession.qr.update(// language=TSQL
                        "UPDATE [FILE] SET STATUS = ?, REGISTERED = ? WHERE ID = ?",
                        *arrayOf(status, Timestamp.valueOf(LocalDateTime.now()), fileID))
                else -> xSession.qr.update(// language=TSQL
                        "UPDATE [FILE] SET STATUS = ? WHERE ID = ?",
                        *arrayOf(status, fileID))
            }
        }

        fun split(): List<ISA> = ISA.read(asFile)

        fun withISA(isa: ISA, block: XISA.() -> Unit) {
            context(XISA.of(this, isa), block)
        }

        private fun context(xISA: XISA, block: XISA.() -> Unit) {
            val xContext = xSession.xContext
            try {
                xSession.xContext = xContext.copy(xISA = xISA)
                xISA.block()
            } finally {
                xSession.xContext = xContext
            }
        }

        companion object {
            fun of(xSession: XSession, file: File): XFile {
                val id: Long
                val digest = file.inputStream().use { stream ->
                    md.apply {
                        val bytes = ByteArray(4096)
                        while (true) {
                            val len = stream.read(bytes)
                            if (len == -1) break
                            update(bytes, 0, len)
                        }
                    }.digest().toHexString()
                }
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

    class XISA internal constructor(
            internal val xSession: XSession,
            val isaID: Long,
            val isaDigest: String,
            isaStatus: String
    ) {
        internal var isaFiles: MutableSet<String> = mutableSetOf()
            set(value) {
                field = value
            }

        var isaStatus: String = isaStatus
            set(value) {
                updateIsaStatus(value)
                field = value
            }

        private fun updateIsaStatus(status: String): Unit {
            //language=TSQL
            xSession.qr.update("UPDATE ISA SET STATUS = ? WHERE ID = ?", *arrayOf(status, isaID))
        }

        companion object {
            fun of(xFile: XFile, isa: ISA): XISA {
                val digest = md.digest(isa.code.toByteArray(ISA.CHARSET)).toHexString()
                return xFile.xSession.xIsaMap[digest] ?: insert(xFile, isa, digest)
            }

            private fun insert(xFile: XFile, isa: ISA, digest: String): XISA {
                val qr = xFile.xSession.qr
                //language=TSQL
                val id: Long = qr.insert(
                        "INSERT INTO ISA (DIGEST, DOC_TYPE, DATE8, TIME8) VALUES (?,?,?,?)",
                        { it.next(); it.getLong(1) },
                        arrayOf<Any?>(
                                digest,
                                isa.stat.doc.type,
                                isa.stat.doc.date,
                                isa.stat.doc.time
                        ))
                //language=TSQL
                qr.insert(
                        "INSERT INTO R_ISA_FILE (ISA_ID, FILE_ID) VALUES (?,?)",
                        {},
                        arrayOf(id, xFile.fileID))

                return XISA(xFile.xSession, id, digest, "").apply {
                    isaFiles.add(xFile.fileDigest)
                    xFile.xSession.xIsaMap[digest] = this
                }
            }
        }
    }

    companion object {
        private val md: MessageDigest get() = MessageDigest.getInstance("SHA")

        private fun SQLServerDataSource.open(): DataSource {
            serverName = properties("serverName")
            portNumber = properties("portNumber").toInt()
            instanceName = properties("instanceName")
            databaseName = properties("databaseName")
            user = properties("user")
            setPassword(properties("password"))
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

        private operator fun Properties.invoke(name: String): String = getProperty(name, null)
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

    private class XLogDB(private val xSession: XSession) : XLog {
        var xContext = XContext(xSession)

        override fun trace(message: String, action: String?, details: String?, detailsXml: String?) {
            log(Level.TRACE, message, action, details, detailsXml)
        }

        override fun info(message: String, action: String?, details: String?, detailsXml: String?) {
            log(Level.INFO, message, action, details, detailsXml)
        }

        override fun warning(message: String, exception: Throwable?, action: String?, details: String?, detailsXml: String?) = log(
                level = Level.WARNING,
                message = message,
                action = action,
                details = details(details, exception),
                detailsXml = detailsXml
        )

        override fun error(message: String, exception: Throwable?, action: String?, details: String?, detailsXml: String?) = log(
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
            System.err.println("${"[$level]".padEnd(10)} $message")
            if (details != null) System.err.println(" details:  $details")
            if (detailsXml != null) System.err.println(" detailsXml: $detailsXml")
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