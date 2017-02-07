package com.simagis.edi.basex

import com.microsoft.sqlserver.jdbc.SQLServerDataSource
import org.apache.commons.dbutils.QueryRunner
import java.io.Closeable
import java.io.File
import java.security.MessageDigest
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Timestamp
import java.util.*

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 2/6/2017.
 */
class XReg(private val sessionUUID: String? = null) : Closeable {
    private val opened = mutableListOf<Closeable>()
    private val qr: QueryRunner by lazy {
        QueryRunner(SQLServerDataSource().apply {
            serverName = properties["serverName"]
            portNumber = properties["portNumber"].toInt()
            instanceName = properties["instanceName"]
            databaseName = properties["databaseName"]
            user = properties["user"]
            setPassword(properties["password"])
        })
    }
    val log: Log = Log(this)
    val session: Session by lazy { Session(this).apply { opened += this } }
    var file: DBFile? = null
        set(value) {
            value?.let { new ->
                //language=TSQL
                val oldID = qr.query("SELECT ID FROM [FILE] WHERE DIGEST = ?", {
                    if (it.next()) it.getLong(1) else null
                }, arrayOf(new.digest))

                var isNewPathEAlreadyExists = false
                if (oldID != null) {
                    new.id = oldID
                    //language=TSQL
                    isNewPathEAlreadyExists = qr.query(
                            "SELECT * FROM FILE_PATH WHERE FILE_ID = ? AND PATH = ?",
                            ResultSet::next, arrayOf(oldID, new.path))
                } else {
                    //language=TSQL
                    new.id = qr.insert(
                            "INSERT INTO [FILE] (DIGEST) VALUES (?)",
                            {
                                it.next()
                                it.getLong(1)
                            },
                            arrayOf<Any?>(new.digest))
                }

                //language=TSQL
                if (!isNewPathEAlreadyExists) {
                    qr.insert(
                            "INSERT INTO FILE_PATH (FILE_ID, PATH, NAME) VALUES (?, ?, ?)", {},
                            arrayOf(new.id, new.path, new.name))
                }
            }
            field = value
        }

    companion object {
        private val md: MessageDigest get() = MessageDigest.getInstance("SHA")
        private val properties: Properties by lazy {
            Properties().apply {
                if (!propertiesFile.isFile) {
                    this["serverName"] = "localhost"
                    this["portNumber"] = "1433"
                    this["instanceName"] = "MSSQLSERVER"
                    this["databaseName"] = "master"
                    this["user"] = "sa"
                    this["password"] = "#PASSWORD"
                    propertiesFile.outputStream().use { store(it, "") }
                }

                propertiesFile.inputStream().use { load(it) }
            }
        }

        private val propertiesFile =
                File(System.getenv("USERPROFILE") ?: ".").resolve(".x-isa.properties")

        private operator fun Properties.get(name: String): String = getProperty(name, null)
                ?: throw SQLException("""property "$name" not found in $propertiesFile""")
    }

    class Log(private val XReg: XReg) {
        fun info(message: String) {
            //language=TSQL
            XReg.qr.insert("""
                    INSERT INTO LOG (
                            LEVEL,
                            SESSION_ID,
                            FILE_ID,
                            ISA_ID,
                            MESSAGE)
                    VALUES (?, ?, ?, ?, ?)""",
                    {},
                    arrayOf(
                            100,
                            XReg.session.id,
                            XReg.file?.id,
                            null,
                            message
                    ))
        }

    }

    class DBFile(private val file: java.io.File) {
        val digest: String = md.digest(file.readBytes()).joinToString(separator = "") {
            Integer.toHexString(it.toInt().and(0xff))
        }

        val path: String get() = file.absolutePath
        val name: String get() = file.name
        var id: Long? = null
            set(value) {
                if (field != null) throw AssertionError()
                field = value
            }
    }

    class Session(private val XReg: XReg) : Closeable {
        val id: Int
        val uuid = XReg.sessionUUID ?: UUID.randomUUID().toString()
        val started = System.currentTimeMillis()

        init {
            //language=TSQL
            id = XReg.qr.insert(
                    "INSERT INTO SESSION (UUID, STARTED) VALUES (?, ?)",
                    {
                        it.next()
                        it.getInt(1)
                    },
                    arrayOf(uuid, Timestamp(started))
            )
        }

        override fun close() {
            //language=TSQL
            XReg.qr.update("UPDATE SESSION SET FINISHED = ?",
                    *arrayOf(Timestamp(System.currentTimeMillis()))
            )
        }
    }

    override fun close() {
        opened.forEach {
            try {
                it.close()
            } catch(e: Exception) {
                e.printStackTrace()
            }
        }
    }

}