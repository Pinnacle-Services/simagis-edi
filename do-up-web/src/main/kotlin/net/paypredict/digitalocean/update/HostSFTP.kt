package net.paypredict.digitalocean.update

import com.jcraft.jsch.*
import java.io.File
import java.io.Reader
import javax.json.Json
import javax.json.JsonObject


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 6/17/2018.
 */

class HostSFTP
private constructor(
    private val host: String,
    private val user: String,
    private val password: String
) {

    fun <T> session(action: ChannelSftp.() -> T): T {
        val jsch = JSch()
        val session: Session = jsch.getSession(user, host, 22)
        session.setConfig("StrictHostKeyChecking", "no")
        session.setPassword(password)
        session.connect()

        val channel: Channel = session.openChannel("sftp")
        channel.connect()
        val sftpChannel = channel as ChannelSftp
        return try {
            sftpChannel.action()
        } finally {
            sftpChannel.exit()
            session.disconnect()
        }
    }

    companion object {
        fun new(): HostSFTP =
            confDir.resolve("HostSFTP.json").reader().use { it.readJson() }.let { json ->
                HostSFTP(
                    host = json.getString("host"),
                    user = json.getString("user"),
                    password = json.getString("password")
                )
            }
    }
}

internal fun Reader.readJson(): JsonObject = Json.createReader(this).readObject()
